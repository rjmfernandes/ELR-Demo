#!/usr/bin/env bash
#
# Captures the evidence for the current step:
#   1. ELR feature level (eligible.leader.replicas.version)
#   2. Partition state from the CONTROLLER quorum: leader, replicas, ISR, ELR,
#      LastKnownElr (works even when every broker is down)
#   3. High watermark (what consumers can see up to)
#   4. Per-replica log-end offset (LEO) and records, read straight from each
#      broker's data volume (works even when that broker is stopped)
#
#   ./status.sh

source "$(dirname "$0")/lib/common.sh"
require_tools

CLUSTER_ID="$(grep '^CLUSTER_ID=' .env | cut -d= -f2)"
VOTERS="101@controller-1:9093,102@controller-2:9093,103@controller-3:9093"

printf "${BOLD}Brokers${NC}     "
for b in a b c; do
    c="$(broker_container "$b")"
    if container_running "$c"; then
        printf "${GREEN}● %s${NC}  " "$(echo "$b" | tr a-z A-Z)"
    else
        printf "${RED}○ %s${NC}  " "$(echo "$b" | tr a-z A-Z)"
    fi
done
printf "   ${DIM}(A=1 B=2 C=3)${NC}\n"

if any_broker_online; then
    printf "${BOLD}ELR feature${NC} eligible.leader.replicas.version = %s\n" "$(elr_feature_level)"
fi

# Controller view of the partition (authoritative metadata).
reg="$(kt kafka-metadata-shell -t "$CLUSTER_ID" -q "$VOTERS" \
        cat "/image/topics/byName/${TOPIC}/${PARTITION}" | grep PartitionRegistration)"
if [ -n "$reg" ]; then
    # get <field>: "isr=[1, 2]" -> "1,2"
    get() { echo "$reg" | sed -n "s/.*[ (]$1=\[\([^]]*\)\].*/\1/p" | tr -d ' '; }
    leader="$(echo "$reg" | sed -n 's/.* leader=\(-\{0,1\}[0-9]*\).*/\1/p')"
    [ "$leader" = "-1" ] && leader_txt="${RED}NONE (offline)${NC}" || leader_txt="${GREEN}$(ids_to_labels "$leader")${NC}"
    printf "${BOLD}Partition${NC}   %s-%s  leader=%b  replicas=%s  isr=%s  ${CYAN}elr=%s${NC}  lastKnownElr=%s  leaderEpoch=%s\n" \
        "$TOPIC" "$PARTITION" "$leader_txt" \
        "$(ids_to_labels "$(get replicas)")" "$(ids_to_labels "$(get isr)")" \
        "$(ids_to_labels "$(get elr)")" "$(ids_to_labels "$(get lastKnownElr)")" \
        "$(echo "$reg" | sed -n 's/.*leaderEpoch=\([0-9]*\).*/\1/p')"
else
    warn "Topic $TOPIC not found in controller metadata."
    exit 0
fi

# High watermark (only answerable through a live leader).
if [ "$leader" != "-1" ]; then
    hwm="$(kt kafka-get-offsets --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --partitions "$PARTITION" \
            | awk -F: '{print $3}')"
    printf "${BOLD}HWM${NC}         %s  ${DIM}(consumers can read offsets < HWM)${NC}\n" "${hwm:-?}"
else
    printf "${BOLD}HWM${NC}         n/a (no leader)\n"
fi

# Per-replica log contents, straight from disk.
printf "${BOLD}Replica logs${NC} ${DIM}(read from each broker volume; LEO = log-end offset)${NC}\n"
for b in a b c; do
    B="$(echo "$b" | tr a-z A-Z)"
    dir="/data/$b/${TOPIC}-${PARTITION}"
    if ! docker exec tools test -d "$dir"; then
        printf "  %s  ${RED}partition directory missing${NC}\n" "$B"
        continue
    fi
    dump="$(kt sh -c "kafka-dump-log --print-data-log --files \$(ls $dir/*.log | tr '\n' ',')")"
    leo="$(echo "$dump" | sed -n 's/^baseOffset: [0-9]* lastOffset: \([0-9]*\).*/\1/p' | tail -1)"
    leo=$(( ${leo:--1} + 1 ))
    ckpt="$(docker exec tools sh -c "grep '^${TOPIC} ${PARTITION} ' /data/$b/replication-offset-checkpoint 2>/dev/null" | awk '{print $3}')"
    printf "  %s  LEO=%s  checkpointed-HWM=%s\n" "$B" "$leo" "${ckpt:-?}"
    echo "$dump" | sed -n 's/^| offset: \([0-9]*\).* payload: \(.*\)$/       offset \1: \2/p'
done

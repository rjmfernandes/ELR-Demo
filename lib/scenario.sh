#!/usr/bin/env bash
# Guided fault sequence shared by run-1-elr-disabled.sh and run-2-elr-enabled.sh.
# Sourced after lib/common.sh. Expects:
#   ELR_MODE      enable | disable
#   RUN_PREFIX    e.g. run1-elr-off
#   RUN_TITLE     banner text
# Optional:
#   RETURN_FIRST  a (default: the stale, wiped last-ISR replica returns first)
#                 | b (alternative: the replica with the committed data returns first)
#   UNCLEAN       false (default) | true  (unclean.leader.election.enable on the topic)
#   PAUSE         1 (default, wait for Enter between steps) | 0

RETURN_FIRST="${RETURN_FIRST:-a}"
export UNCLEAN="${UNCLEAN:-false}"

mkdir -p "$EVIDENCE_DIR"
LOG_FILE="$EVIDENCE_DIR/${RUN_PREFIX}-$(date '+%Y%m%d-%H%M%S').log"
# Everything printed below is also captured as evidence.
exec > >(tee -a "$LOG_FILE") 2>&1

banner "$RUN_TITLE"
info "Evidence log: ${LOG_FILE#$DEMO_DIR/}"
info "Settings: ELR=$ELR_MODE  unclean.leader.election.enable=$UNCLEAN  first broker back=$(echo "$RETURN_FIRST" | tr a-z A-Z)"


# ---------------------------------------------------------------------------
step "1/5" "FRESH CLUSTER, ELR FEATURE, TOPIC  (ISR {A,B,C})"
# ---------------------------------------------------------------------------

./start-cluster.sh || exit 1
./set-elr.sh "$ELR_MODE" || exit 1
./create-topic.sh "$RUN_PREFIX" || exit 1
printf '\n'; ./status.sh
pause


# ---------------------------------------------------------------------------
step "2/5" "STOP C  ->  ISR {A,B}  ->  COMMITTED CHECKPOINT (acks=all)"
# ---------------------------------------------------------------------------

./stop-broker.sh c
wait_for_isr "1,2" 60 || exit 1
printf '\n'
./produce.sh all CHECKPOINT
printf '\n'; ./consume.sh
printf '\n'; ./status.sh
pause


# ---------------------------------------------------------------------------
step "3/5" "STOP B  ->  ISR {A}  (below min.insync.replicas=2)  ->  LOWER-ACK WRITES"
# ---------------------------------------------------------------------------

./stop-broker.sh b
wait_for_isr "1" 60 || exit 1
printf '\n'; ./status.sh
if [ "$ELR_MODE" = "enable" ]; then
    info "Expected: B is listed in elr= because it left the ISR holding all data up to the (now frozen) HWM."
else
    info "Expected: elr= is empty, the controller does not track eligible replicas when the feature is 0."
fi

printf '\n'
./produce.sh 1 TEST-ACKS1
./produce.sh 0 TEST-ACKS0
./produce.sh all TEST-ACKSALL
info "Expected: acks=all fails with NotEnoughReplicasException while ISR < 2."

printf '\n'; ./consume.sh
printf '\n'; ./status.sh
info "Compare A's LEO with the HWM: lower-ack records sit in A's log above the HWM and are invisible to consumers."
pause


# ---------------------------------------------------------------------------
step "4/5" "KILL A (unclean)  ->  SIMULATE LOSS OF A's PARTITION DATA"
# ---------------------------------------------------------------------------

./stop-broker.sh a --unclean
info "Waiting 15s so the controller fences A (broker.session.timeout.ms=9s)..."
sleep 15
printf '\n'; ./status.sh
printf '\n'
./wipe-partition.sh a || exit 1
pause


# ---------------------------------------------------------------------------
step "5/5" "RECOVERY  (broker $(echo "$RETURN_FIRST" | tr a-z A-Z) comes back first)"
# ---------------------------------------------------------------------------

if [ "$RETURN_FIRST" = "a" ]; then
    first=a; second=b
else
    first=b; second=a
fi

./start-broker.sh "$first"
if ! wait_for_any_leader 30; then
    if [ "$UNCLEAN" = "true" ]; then
        info "No leader yet: asking the controller for the (configured) unclean election explicitly."
        ./elect-unclean.sh
        wait_for_any_leader 30
    else
        warn "No leader and unclean election is disabled: the partition waits for a safe replica (unavailability, not data loss)."
    fi
fi
printf '\n'; ./status.sh
printf '\n'; ./consume.sh

printf '\n'
show_partition_changes
if [ "$first" = "a" ] && [ "$ELR_MODE" = "disable" ]; then
    info "Expected: A was still the only ISR member, so it was elected CLEANLY even though its log is empty."
    info "The committed CHECKPOINT is no longer readable, and no unclean election was reported."
elif [ "$first" = "a" ]; then
    info "Expected: A restarted after an unclean shutdown, so the controller removed it from ELR."
    info "B (still in ELR) is offline, so the partition waits: no leader for now, but no data loss."
elif [ "$ELR_MODE" = "disable" ]; then
    info "Expected: B is not in the ISR, so it could only be elected through an UNCLEAN election (UNCLEAN=true)."
else
    info "Expected: B is in ELR, so it was elected CLEANLY: no unclean election needed."
fi
pause

printf '\n'
info "Bringing the remaining broker $(echo "$second" | tr a-z A-Z) back."
./start-broker.sh "$second"
wait_for_any_leader 60
info "Letting the follower catch up (it truncates its log to match the leader)..."
sleep 10
printf '\n'; ./status.sh
printf '\n'; ./consume.sh
show_partition_changes
if [ "$first" = "a" ] && [ "$ELR_MODE" = "disable" ]; then
    info "Expected: B rejoined as a follower of A and truncated its log to match: the CHECKPOINT is gone from every replica."
elif [ "$first" = "a" ]; then
    info "Expected: B was elected from ELR (a clean election) and A rejoined as its follower: the CHECKPOINT survived."
fi


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

rid="$(run_id)"
visible="$(./consume.sh 2>/dev/null)"

banner "SUMMARY  ($rid)"
check() {
    if echo "$visible" | grep -q "|$1|"; then
        printf "  %-14s ${GREEN}present${NC}\n" "$1"
    else
        printf "  %-14s ${RED}missing${NC}\n" "$1"
    fi
}
check CHECKPOINT
check TEST-ACKS1
check TEST-ACKS0
printf '\n'
if [ "$first" = "a" ]; then
    info "Note: had B come back first, B would have been elected and the CHECKPOINT kept in both runs."
    info "With ELR disabled, though, that only happens through an unclean election (UNCLEAN=true),"
    info "because B is not in the ISR. Try it: RETURN_FIRST=b UNCLEAN=true ./run-1-elr-disabled.sh"
    printf '\n'
fi
info "Full evidence: ${LOG_FILE#$DEMO_DIR/}"

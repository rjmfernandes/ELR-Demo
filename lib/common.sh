#!/usr/bin/env bash
# Shared helpers for the ELR demo scripts. Sourced, not executed.
#
# Every Kafka CLI command runs inside the "tools" container, which is never
# stopped during the demo, so commands keep working while brokers are down.
# Every wait loop has a deadline: nothing in this demo can hang forever.

set -uo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DEMO_DIR"

TOPIC="${TOPIC:-elr-demo}"
PARTITION=0
BOOTSTRAP="kafka-a:29092,kafka-b:29092,kafka-c:29092"
RUN_ID_FILE="$DEMO_DIR/.run-id"
EVIDENCE_DIR="$DEMO_DIR/evidence"


# ---------------------------------------------------------------------------
# Terminal colors
# ---------------------------------------------------------------------------

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

separator() {
    printf '%s\n' "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

banner() {
    printf '\n'; separator
    printf "${BOLD}  %s${NC}\n" "$1"
    separator; printf '\n'
}

step() {
    printf '\n'
    printf "${CYAN}${BOLD}[%s] %s${NC}\n" "$1" "$2"
    printf '\n'
}

success() { printf "${GREEN}✓${NC} %s\n" "$1"; }
failure() { printf "${RED}✗${NC} %s\n" "$1"; }
warn()    { printf "${YELLOW}!${NC} %s\n" "$1"; }
info()    { printf "${DIM}%s${NC}\n" "$1"; }

die() {
    failure "$1"
    exit 1
}

# Wait for Enter between steps of the guided runs (PAUSE=0 disables it).
pause() {
    if [ "${PAUSE:-1}" = "1" ]; then
        printf "\n${DIM}Press Enter to continue...${NC}"
        read -r _
    fi
}


# ---------------------------------------------------------------------------
# Broker label <-> container / node id
# ---------------------------------------------------------------------------

broker_container() {
    case "$1" in
        a|A) echo kafka-a ;;
        b|B) echo kafka-b ;;
        c|C) echo kafka-c ;;
        *) die "Unknown broker '$1' (expected a, b or c)" ;;
    esac
}

broker_id() {
    case "$1" in
        a|A) echo 1 ;;
        b|B) echo 2 ;;
        c|C) echo 3 ;;
        *) die "Unknown broker '$1' (expected a, b or c)" ;;
    esac
}

# Translates a list of node ids ("1,2") into labels ("A,B").
ids_to_labels() {
    echo "$1" | sed -e 's/1/A/g' -e 's/2/B/g' -e 's/3/C/g'
}

container_running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}


# ---------------------------------------------------------------------------
# Kafka CLI wrappers
# ---------------------------------------------------------------------------

# Runs a command in the tools container, dropping client WARN noise (e.g. DNS
# failures for brokers that are intentionally stopped).
kt() {
    docker exec tools "$@" 2>&1 | grep -v -E ' WARN |^SLF4J'
}

# Same, with stdin attached (producer).
kti() {
    docker exec -i tools "$@" 2>&1 | grep -v -E ' WARN |^SLF4J'
}

require_tools() {
    container_running tools || die "The tools container is not running. Run ./start-cluster.sh first."
}

any_broker_online() {
    container_running kafka-a || container_running kafka-b || container_running kafka-c
}

elr_feature_level() {
    kt kafka-features --bootstrap-server "$BOOTSTRAP" describe \
        | awk '/eligible.leader.replicas.version/ {
                 for (i = 1; i <= NF; i++) if ($i == "FinalizedVersionLevel:") print $(i + 1)
               }'
}

# Prints the raw "Partition: 0 ..." describe line (empty if no broker answers).
partition_line() {
    any_broker_online || return 0
    kt kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC" \
        | grep -E "Partition: ${PARTITION}[[:space:]]" | head -1
}

# Extracts a field (Leader, Replicas, Isr, Elr, LastKnownElr) from a describe line.
field() {
    echo "$1" | tr '\t' '\n' | sed -n "s/^[[:space:]]*$2: *//p" | head -1
}

current_leader() { field "$(partition_line)" Leader; }
current_isr()    { field "$(partition_line)" Isr; }
current_elr()    { field "$(partition_line)" Elr; }

# Waits until the ISR (as node ids, e.g. "1,2") equals $1. Deadline: $2 seconds.
wait_for_isr() {
    local expected="$1" timeout="${2:-60}" deadline=$((SECONDS + ${2:-60})) isr=""
    info "Waiting up to ${timeout}s for ISR {$(ids_to_labels "$expected")}..."
    while [ $SECONDS -lt $deadline ]; do
        isr="$(current_isr)"
        if [ "$isr" = "$expected" ]; then
            success "ISR is {$(ids_to_labels "$isr")}"
            return 0
        fi
        sleep 2
    done
    warn "ISR is {$(ids_to_labels "$isr")} after ${timeout}s (expected {$(ids_to_labels "$expected")})"
    return 1
}

# Waits until the partition has a leader (any). Deadline: $1 seconds.
wait_for_any_leader() {
    local timeout="${1:-60}" deadline=$((SECONDS + ${1:-60})) leader=""
    info "Waiting up to ${timeout}s for a partition leader..."
    while [ $SECONDS -lt $deadline ]; do
        leader="$(current_leader)"
        if [ -n "$leader" ] && [ "$leader" != "none" ] && [ "$leader" != "-1" ]; then
            success "Partition leader is broker $(ids_to_labels "$leader") (node $leader)"
            return 0
        fi
        sleep 3
    done
    warn "No leader after ${timeout}s"
    return 1
}

# Waits until broker $1 (a|b|c) answers API requests. Deadline: $2 seconds.
wait_for_broker() {
    local container timeout="${2:-120}" deadline=$((SECONDS + ${2:-120}))
    container="$(broker_container "$1")"
    info "Waiting up to ${timeout}s for $container to answer..."
    while [ $SECONDS -lt $deadline ]; do
        if docker exec tools kafka-broker-api-versions --bootstrap-server "$container:29092" \
                >/dev/null 2>&1; then
            success "$container is up"
            return 0
        fi
        sleep 3
    done
    warn "$container did not answer within ${timeout}s"
    return 1
}


# ---------------------------------------------------------------------------
# Run id
# ---------------------------------------------------------------------------

new_run_id() {
    local prefix="${1:-run}"
    echo "${prefix}-$(date '+%Y%m%d-%H%M%S')" > "$RUN_ID_FILE"
    cat "$RUN_ID_FILE"
}

run_id() {
    [ -f "$RUN_ID_FILE" ] || die "No run id found. Run ./create-topic.sh first."
    cat "$RUN_ID_FILE"
}


# ---------------------------------------------------------------------------
# Controller log: unclean leader elections of the demo partition.
# At INFO level the controller only logs UNCLEAN partition changes; clean
# elections (including election from ELR) leave no line here.
# ---------------------------------------------------------------------------

unclean_elections() {
    # Every controller replays the record and logs it; keep one copy per change.
    for c in controller-1 controller-2 controller-3; do
        docker logs "$c" 2>&1 | grep -E "UNCLEAN partition change for ${TOPIC}-${PARTITION} " \
            | sed -E 's/^\[([^]]*)\][^]]*\] UNCLEAN partition change for [^:]*: (.*)$/\1  \2/' \
            | sed -E 's/,[0-9]{3}  /  /'
    done | sort -u -k1,2
}

show_partition_changes() {
    local lines
    lines="$(unclean_elections)"
    printf "${BOLD}Unclean leader elections for %s-%s${NC} ${DIM}(from the controller logs)${NC}\n" "$TOPIC" "$PARTITION"
    if [ -n "$lines" ]; then
        echo "$lines" | sed "s/^/  $(printf "$RED")UNCLEAN$(printf "$NC")  /"
    else
        printf "  ${GREEN}none${NC} ${DIM}- every leader election for this partition was clean${NC}\n"
    fi
}

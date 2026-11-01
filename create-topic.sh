#!/usr/bin/env bash
#
# Creates the single-partition demo topic:
#   RF=3, replica order A,B,C (A = preferred leader), min.insync.replicas=2,
#   unclean.leader.election.enable=false  (override with UNCLEAN=true)
#
# Also generates a new run id (stored in .run-id) that is embedded in every record.
#
#   ./create-topic.sh run1-elr-off

source "$(dirname "$0")/lib/common.sh"
require_tools

UNCLEAN="${UNCLEAN:-false}"

kt kafka-topics --bootstrap-server "$BOOTSTRAP" --create --topic "$TOPIC" \
    --replica-assignment 1:2:3 \
    --config min.insync.replicas=2 \
    --config "unclean.leader.election.enable=$UNCLEAN"

wait_for_isr "1,2,3" 30 || die "Topic did not reach ISR {A,B,C}"

rid="$(new_run_id "${1:-run}")"
success "Run id: $rid"
printf '\n'
kt kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"

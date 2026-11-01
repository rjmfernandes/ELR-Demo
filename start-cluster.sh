#!/usr/bin/env bash
#
# Starts a FRESH cluster: every volume is deleted first (docker compose down -v),
# so each run begins from identical, empty state.
#
#   ./start-cluster.sh

source "$(dirname "$0")/lib/common.sh"

banner "STARTING A FRESH ELR DEMO CLUSTER"

info "Removing any previous containers and volumes..."
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
rm -f "$RUN_ID_FILE"

docker compose up -d

printf '\n'
for b in a b c; do
    wait_for_broker "$b" 180 || die "Broker $b did not start. Check: docker compose logs kafka-$b"
done

printf '\n'
kt kafka-metadata-quorum --bootstrap-controller controller-1:9093 describe --status \
    | grep -E 'LeaderId|CurrentVoters'

printf '\n'
success "Cluster ready. ELR feature level: $(elr_feature_level)"

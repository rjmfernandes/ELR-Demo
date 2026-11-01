#!/usr/bin/env bash
#
# Sets the cluster-wide ELR feature level and prints the result.
#
#   ./set-elr.sh disable   # eligible.leader.replicas.version=0  (Run 1)
#   ./set-elr.sh enable    # eligible.leader.replicas.version=1  (Run 2, default on new CP 8.1 clusters)
#
# Do this BEFORE creating the topic / starting the fault sequence.

source "$(dirname "$0")/lib/common.sh"
require_tools

case "${1:-}" in
    enable)  level=1; verb=upgrade ;;
    disable) level=0; verb=downgrade ;;
    *) die "Usage: $0 enable|disable" ;;
esac

current="$(elr_feature_level)"
if [ "$current" != "$level" ]; then
    kt kafka-features --bootstrap-server "$BOOTSTRAP" "$verb" \
        --feature "eligible.leader.replicas.version=$level"
fi

current="$(elr_feature_level)"
[ "$current" = "$level" ] || die "eligible.leader.replicas.version is $current (expected $level)"
success "eligible.leader.replicas.version FinalizedVersionLevel = $current"

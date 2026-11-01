#!/usr/bin/env bash
#
# Explicitly requests an UNCLEAN leader election for the demo partition.
# The controller normally does this by itself (unclean.leader.election.enable=true
# on the topic); this script just makes it immediate and visible.
#
#   ./elect-unclean.sh

source "$(dirname "$0")/lib/common.sh"
require_tools

kt kafka-leader-election --bootstrap-server "$BOOTSTRAP" \
    --election-type UNCLEAN --topic "$TOPIC" --partition "$PARTITION"

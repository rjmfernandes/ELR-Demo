#!/usr/bin/env bash
#
# Lists the unclean leader elections the controller performed for the demo
# partition (or "none" if every election was clean, e.g. from ELR).
#
#   ./show-elections.sh

source "$(dirname "$0")/lib/common.sh"

show_partition_changes

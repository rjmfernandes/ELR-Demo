#!/usr/bin/env bash
#
# Run 1 - ELR DISABLED (eligible.leader.replicas.version=0)
#
#   ./run-1-elr-disabled.sh                              # stale, wiped A comes back first
#   RETURN_FIRST=b UNCLEAN=true ./run-1-elr-disabled.sh  # alternative: B comes back first
#   PAUSE=0 ./run-1-elr-disabled.sh                      # no Enter prompts between steps

source "$(dirname "$0")/lib/common.sh"

ELR_MODE=disable
RUN_PREFIX="run1-elr-off"
RUN_TITLE="RUN 1 - ELR DISABLED"

source "$DEMO_DIR/lib/scenario.sh"

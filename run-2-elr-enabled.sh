#!/usr/bin/env bash
#
# Run 2 - ELR ENABLED (eligible.leader.replicas.version=1)
#
#   ./run-2-elr-enabled.sh                               # stale, wiped A comes back first
#   RETURN_FIRST=b UNCLEAN=true ./run-2-elr-enabled.sh   # alternative: B comes back first
#   PAUSE=0 ./run-2-elr-enabled.sh                       # no Enter prompts between steps

source "$(dirname "$0")/lib/common.sh"

ELR_MODE=enable
RUN_PREFIX="run2-elr-on"
RUN_TITLE="RUN 2 - ELR ENABLED"

source "$DEMO_DIR/lib/scenario.sh"

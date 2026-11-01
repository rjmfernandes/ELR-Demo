#!/usr/bin/env bash
#
# Stops one broker.
#
#   ./stop-broker.sh c             # clean shutdown (SIGTERM, controlled shutdown)
#   ./stop-broker.sh a --unclean   # hard kill (SIGKILL): simulates a crash
#
# A clean shutdown matters for ELR: a broker that restarts after an UNCLEAN
# shutdown is removed from ELR by the controller, because its log may be
# missing data it previously acknowledged.

source "$(dirname "$0")/lib/common.sh"

label="${1:-}"
mode="${2:---clean}"
[ -n "$label" ] || die "Usage: $0 <a|b|c> [--clean|--unclean]"
container="$(broker_container "$label")"

container_running "$container" || die "$container is already stopped."

if [ "$mode" = "--unclean" ]; then
    printf "${RED}${BOLD}Killing %s (SIGKILL, unclean shutdown)${NC}\n" "$container"
    docker kill "$container" >/dev/null
else
    printf "${YELLOW}Stopping %s (SIGTERM, clean shutdown)${NC}\n" "$container"
    docker stop "$container" >/dev/null
fi
success "$container stopped."

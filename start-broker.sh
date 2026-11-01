#!/usr/bin/env bash
#
# Starts one previously stopped broker and waits for it to answer.
#
#   ./start-broker.sh b

source "$(dirname "$0")/lib/common.sh"

label="${1:-}"
[ -n "$label" ] || die "Usage: $0 <a|b|c>"
container="$(broker_container "$label")"

container_running "$container" && die "$container is already running."

docker start "$container" >/dev/null
wait_for_broker "$label" 120

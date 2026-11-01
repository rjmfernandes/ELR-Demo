#!/usr/bin/env bash
#
# DESTRUCTIVE (disposable lab only): simulates disk loss of ONE topic-partition
# on ONE stopped broker. Only the demo partition directory is touched; the rest
# of the broker volume (metadata, internal topics) is left intact.
#
#   ./wipe-partition.sh a
#
# Safety:
#   - refuses to run if the broker is running
#   - copies the partition directory into ./backups/ first

source "$(dirname "$0")/lib/common.sh"
require_tools

label="${1:-}"
[ -n "$label" ] || die "Usage: $0 <a|b|c>"
container="$(broker_container "$label")"
lower="$(echo "$label" | tr 'A-Z' 'a-z')"
dir="/data/${lower}/${TOPIC}-${PARTITION}"

container_running "$container" && die "$container is running. Stop it first: never wipe a live broker volume."

docker exec tools test -d "$dir" || die "$dir does not exist on $container's volume."

backup="/backups/${container}-${TOPIC}-${PARTITION}-$(date '+%Y%m%d-%H%M%S')"
docker exec tools cp -r "$dir" "$backup" || die "Backup failed, not wiping."
success "Backup written to .${backup}"

docker exec tools rm -rf "$dir" || die "Wipe failed."
printf "${RED}${BOLD}Partition %s-%s deleted from %s's volume${NC}\n" "$TOPIC" "$PARTITION" "$container"

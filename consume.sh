#!/usr/bin/env bash
#
# Shows what a consumer can see: every record from offset 0 up to the high
# watermark (HWM). No consumer group is used, so nothing depends on
# __consumer_offsets being available while brokers are down.
#
#   ./consume.sh

source "$(dirname "$0")/lib/common.sh"
require_tools

any_broker_online || { warn "No broker online: nothing is consumable (partition offline)."; exit 0; }

leader="$(current_leader)"
if [ -z "$leader" ] || [ "$leader" = "none" ] || [ "$leader" = "-1" ]; then
    warn "Partition has no leader: nothing is consumable right now (this is unavailability, not data loss)."
    exit 0
fi

hwm="$(kt kafka-get-offsets --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --partitions "$PARTITION" \
        | awk -F: '{print $3}')"

printf "Consumer view  (leader %s, HWM = %s)\n" "$(ids_to_labels "$leader")" "${hwm:-?}"
separator

records="$(
    kt kafka-console-consumer \
        --bootstrap-server "$BOOTSTRAP" \
        --topic "$TOPIC" \
        --partition "$PARTITION" \
        --offset earliest \
        --timeout-ms 8000 \
        --property print.offset=true \
        | grep -v -E 'ERROR Error processing message|TimeoutException|Processed a total of'
)"
if [ -n "$records" ]; then
    echo "$records" | sed 's/^/  /'
else
    printf "  ${YELLOW}(no records visible)${NC}\n"
fi
separator

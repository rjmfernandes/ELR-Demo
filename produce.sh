#!/usr/bin/env bash
#
# Sends ONE uniquely marked record and reports the producer result.
#
#   ./produce.sh <acks> <label>
#   ./produce.sh all CHECKPOINT
#   ./produce.sh 1   TEST-ACKS1
#   ./produce.sh 0   TEST-ACKS0
#
# Record value: <run-id>|<label>|acks=<acks>|<time>
#
# Producer result and consumer visibility are DIFFERENT signals:
#   acks=all -> fails with NotEnoughReplicasException while ISR < min.insync.replicas
#   acks=1   -> leader appends and acknowledges; the record may still be invisible
#               to consumers if the high watermark (HWM) is frozen
#   acks=0   -> no acknowledgement at all, nothing to inspect on the producer side

source "$(dirname "$0")/lib/common.sh"
require_tools

ACKS="${1:-}"
LABEL="${2:-}"
[ -n "$ACKS" ] && [ -n "$LABEL" ] || die "Usage: $0 <all|1|0> <label>"

value="$(run_id)|${LABEL}|acks=${ACKS}|$(date '+%H:%M:%S')"

printf "Producing  ${BOLD}%s${NC}\n" "$value"

# Short timeouts so a failing acks=all send reports its error within ~10s
# instead of retrying for the default 2 minutes.
output="$(
    echo "$value" | kti kafka-console-producer \
        --bootstrap-server "$BOOTSTRAP" \
        --topic "$TOPIC" \
        --producer-property "acks=$ACKS" \
        --producer-property request.timeout.ms=5000 \
        --producer-property delivery.timeout.ms=10000 \
        --producer-property linger.ms=0
)"

# kafka-console-producer can exit 0 even when the asynchronous send callback
# reports an error, so stderr is checked for an ERROR line.
if echo "$output" | grep -q 'ERROR'; then
    error_class="$(echo "$output" | grep -oE '[A-Za-z]+Exception' | head -1)"
    failure "acks=$ACKS  FAILED  ${error_class:-unknown error}"
    echo "$output" | grep -E 'Exception:' | head -1 | sed 's/^/    /'
    exit 2
fi

if [ "$ACKS" = "0" ]; then
    warn "acks=0  SENT  (fire-and-forget: there is no acknowledgement to inspect)"
else
    success "acks=$ACKS  ACKNOWLEDGED by the leader"
fi

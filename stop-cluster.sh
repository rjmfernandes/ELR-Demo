#!/usr/bin/env bash
#
# Stops everything and deletes all volumes (all demo data is lost).
#
#   ./stop-cluster.sh            # containers + volumes
#   ./stop-cluster.sh --purge    # also empties ./backups and ./evidence

source "$(dirname "$0")/lib/common.sh"

docker compose down -v --remove-orphans
rm -f "$RUN_ID_FILE"
success "Cluster removed (containers + volumes)."

if [ "${1:-}" = "--purge" ]; then
    # find instead of "rm -rf dir/*": no shell glob, so no interactive
    # "sure you want to delete all files?" prompt in zsh. .gitkeep is kept.
    find "$DEMO_DIR/backups" "$DEMO_DIR/evidence" -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
    success "Emptied ./backups and ./evidence."
fi

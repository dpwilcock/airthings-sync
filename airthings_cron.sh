#!/bin/bash
# Daily Airthings cron job — runs at 5am, fetches last 48h, zero tokens
# Crontab entry: 0 5 * * * /path/to/airthings/airthings_cron.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="/tmp/airthings_cron.log"
PY=/usr/bin/python3

# Append timestamp header
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG"

# Run recent sync (last 48h to catch any missed data)
$PY "$SCRIPT_DIR/airthings_sync.py" --recent --hours 48 >> "$LOG" 2>&1

echo "=== Done $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG"

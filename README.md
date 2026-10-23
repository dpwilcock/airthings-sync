# Airthings Sync

Synchronize Airthings View Plus (and other Airthings) sensor data into PostgreSQL.

Pulls Airthings sensor samples (temperature, humidity, CO2, VOC, radon, pressure) from the [Airthings Ext API](https://developer.airthings.com/) into PostgreSQL. Supports incremental resume (cursor-based), full backfill, last-48h cron runs, per-device sync, targeted gap-filling, and a sync-state table.

## Requirements

- Python 3.9+
- `pip install -r requirements.txt`
- Airthings API client ID + secret (free developer account)
- PostgreSQL database

## Configuration (environment variables)

| Variable | Purpose |
|----------|---------|
| `AIRTHINGS_CLIENT_ID` | Airthings API client ID |
| `AIRTHINGS_CLIENT_SECRET` | Airthings API client secret |
| `PG_DSN` | PostgreSQL DSN (default: `dbname=postgres host=localhost`) |
| `AIRTHINGS_EXCLUDED_DEVICES` | Comma-separated device IDs to skip (optional) |
| `LOG_FILE` | Log path (default `~/logs/airthings_sync.log`) |

## Setup

```bash
psql -U postgres -d postgres -f schema.sql
```

## Usage

```bash
python3 airthings_sync.py            # Incremental (resume from last cursor)
python3 airthings_sync.py --full     # Force full re-download (oldest first)
python3 airthings_sync.py --recent   # Past 48h only (for cron)
python3 airthings_sync.py --status   # Show sync state
python3 airthings_sync.py --device X # Sync only device X
```

### Gap-filling

Backfill a specific date range on one device without cursor-crawling through everything:

```bash
python3 airthings_sync.py --device X --gap-start 2026-01-03 --gap-end 2026-04-29
```

Both dates are required, and gaps are filled with auto-resume: re-running the same command continues from the last inserted point (useful if a run is interrupted by rate limits).

## Schema

The schema (schema.sql) sets up:

- `airthings_devices` — device registry
- `airthings_samples` — sensor samples (idempotent upserts: `ON CONFLICT (device_id, recorded_at) DO NOTHING`)
- `airthings_sync_state` — per-device cursor state for incremental resume

## Cron

See `airthings_cron.sh` for a daily 48h-backfill pattern (edit the path + crontab line to match your machine).
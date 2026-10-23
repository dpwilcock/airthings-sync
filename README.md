# Airthings Sync

Synchronize Airthings View Plus (and other Airthings) sensor data into PostgreSQL.

Pulls Airthings sensor samples (temperature, humidity, CO2, VOC, radon, pressure) from the [Airthings Ext API](https://developer.airthings.com/) into PostgreSQL. Supports incremental resume (cursor-based), full backfill, last-48h cron runs, per-device sync, and a sync-state table.

## Requirements

- Python 3.9+
- `pip install requests psycopg2-binary`
- Airthings API client ID + secret (free developer account)
- PostgreSQL database

## Configuration (environment variables)

| Variable | Purpose |
|----------|---------|
| `AIRTHINGS_CLIENT_ID` | Airthings API client ID |
| `AIRTHINGS_CLIENT_SECRET` | Airthings API client secret |
| `PG_DSN` | PostgreSQL DSN (default: `dbname=postgres host=localhost`) |

## Usage

```bash
python3 airthings_sync.py            # Incremental (resume from last cursor)
python3 airthings_sync.py --full     # Force full re-download (oldest first)
python3 airthings_sync.py --recent   # Past 48h only (for cron)
python3 airthings_sync.py --status   # Show sync state
python3 airthings_sync.py --device X # Sync only device X
```

## Schema

Create the tables first:

```bash
psql -U postgres -d postgres -f schema.sql
```

Sets up `airthings_devices` and `airthings_samples` with a sync-state table for cursor-based resume.

## Cron

See `airthings_cron.sh` for a daily 48h-backfill pattern (edit the path + crontab line to match your machine).
#!/usr/bin/env python3
"""
Airthings View Plus → PostgreSQL sync tool.

Usage:
    python3 airthings_sync.py            # Incremental (resume from last cursor)
    python3 airthings_sync.py --full     # Force full re-download (oldest first)
    python3 airthings_sync.py --recent   # Past 48h only (for cron jobs)
    python3 airthings_sync.py --status   # Show sync state
    python3 airthings_sync.py --device X # Sync only device X
"""

import argparse
import base64
import json
import logging
import time
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
import requests


def get_secret(name):
    """Read a secret from the environment (swap in your own secret store)."""
    return os.environ[name]


def init_auth():
    """Load API credentials. Called after arg parsing so --help/-h never needs secrets."""
    global CLIENT_ID, CLIENT_SECRET
    CLIENT_ID     = get_secret("AIRTHINGS_CLIENT_ID")
    CLIENT_SECRET = get_secret("AIRTHINGS_CLIENT_SECRET")


# ── Config ────────────────────────────────────────────────────────────────────
TOKEN_URL  = "https://accounts-api.airthings.com/v1/token"
API_BASE   = "https://ext-api.airthings.com/v1"
DB_DSN     = os.environ.get("PG_DSN", "dbname=postgres host=localhost")
LOG_FILE   = os.environ.get("LOG_FILE", os.path.expanduser("~/logs/airthings_sync.log"))

try:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
except OSError:
    pass

SAMPLES_PER_PAGE = 1000
MAX_PAGES_PER_RUN = 80   # cap per-device pages per invocation so one device can't hog the whole run / crash into rate-limit death spiral
MAX_RETRY_ATTEMPTS = 6   # lower than before — fail fast and gracefully instead of burning ~35min into a crash

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ]
)
log = logging.getLogger("airthings_sync")

# ── Auth ──────────────────────────────────────────────────────────────────────
_token_cache: dict = {}

def get_token() -> str:
    now = time.time()
    if _token_cache.get("expires_at", 0) - 60 > now:
        return _token_cache["access_token"]
    log.info("Fetching new OAuth token …")
    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"]   = now + data.get("expires_in", 3600)
    return _token_cache["access_token"]


class RateLimitExhausted(RuntimeError):
    """Raised when we give up on a request after repeated 429s — caught per-device so one
    device's rate-limit death spiral can't kill the whole run."""
    pass


def api_get(path: str, params: dict = None) -> dict:
    """GET from Airthings API with exponential backoff on 429/5xx."""
    base_wait = 15  # seconds — start conservative
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            token = get_token()
            resp = requests.get(
                f"{API_BASE}{path}",
                headers={"Authorization": f"Bearer {token}"},
                params=params or {},
                timeout=30,
            )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", base_wait))
                wait = max(retry_after, base_wait * (2 ** attempt))
                wait = min(wait, 300)  # cap at 5 minutes
                log.warning("Rate limited (attempt %d) — sleeping %ds", attempt + 1, wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = base_wait * (2 ** attempt)
                wait = min(wait, 300)
                log.warning("Server error %d (attempt %d) — sleeping %ds",
                            resp.status_code, attempt + 1, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            wait = base_wait * (2 ** attempt)
            log.warning("Timeout (attempt %d) — sleeping %ds", attempt + 1, wait)
            time.sleep(wait)
    raise RateLimitExhausted(f"API request to {path} failed after {MAX_RETRY_ATTEMPTS} attempts")


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(DB_DSN)


def upsert_device(cur, device: dict):
    cur.execute("""
        INSERT INTO climate.airthings_devices
            (device_id, device_type, product_name, segment_id, segment_name,
             location_id, location_name, sensors, segment_started, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (device_id) DO UPDATE SET
            device_type   = EXCLUDED.device_type,
            product_name  = EXCLUDED.product_name,
            segment_name  = EXCLUDED.segment_name,
            location_name = EXCLUDED.location_name,
            sensors       = EXCLUDED.sensors,
            updated_at    = NOW()
    """, (
        device["id"],
        device.get("deviceType"),
        device.get("productName"),
        device["segment"]["id"],
        device["segment"]["name"],
        device["location"]["id"],
        device["location"]["name"],
        device.get("sensors", []),
        device["segment"]["started"],
    ))


def insert_samples(cur, device_id: str, records: list) -> int:
    if not records:
        return 0
    psycopg2.extras.execute_values(cur, """
        INSERT INTO climate.airthings_samples
            (device_id, recorded_at, radon_short_term_avg, temp, humidity,
             pressure, co2, voc, pm1, pm25)
        VALUES %s
        ON CONFLICT (device_id, recorded_at) DO NOTHING
    """, [(
        device_id,
        r["recorded_at"],
        r.get("radonShortTermAvg"),
        r.get("temp"),
        r.get("humidity"),
        r.get("pressure"),
        r.get("co2"),
        r.get("voc"),
        r.get("pm1"),
        r.get("pm25"),
    ) for r in records], page_size=500)
    return cur.rowcount


def save_sync_state(cur, device_id: str, cursor: Optional[str], total: int):
    cur.execute("""
        INSERT INTO climate.airthings_sync_state (device_id, last_cursor, last_sync_at, total_rows)
        VALUES (%s, %s, NOW(), %s)
        ON CONFLICT (device_id) DO UPDATE SET
            last_cursor  = EXCLUDED.last_cursor,
            last_sync_at = NOW(),
            total_rows   = EXCLUDED.total_rows
    """, (device_id, cursor, total))


def get_sync_state(cur, device_id: str) -> dict:
    cur.execute(
        "SELECT last_cursor, last_sync_at, total_rows FROM climate.airthings_sync_state WHERE device_id=%s",
        (device_id,)
    )
    row = cur.fetchone()
    if row:
        return {"cursor": row[0], "last_sync_at": row[1], "total_rows": row[2]}
    return {}


def get_newest_ts(cur, device_id: str) -> Optional[datetime]:
    cur.execute(
        "SELECT MAX(recorded_at) FROM climate.airthings_samples WHERE device_id=%s",
        (device_id,)
    )
    row = cur.fetchone()
    return row[0] if row else None


# ── Sample decoder ────────────────────────────────────────────────────────────
def decode_samples_response(resp: dict) -> list:
    sensors = ["radonShortTermAvg", "temp", "humidity", "pressure", "co2", "voc", "pm1", "pm25"]
    inner = resp.get("data", resp)

    # Airthings API returns parallel sensor arrays.
    # If 'time' array is missing, calculate timestamps from start/end.
    times_unix = inner.get("time", [])
    if not times_unix:
        start_str = inner.get("start")
        end_str   = inner.get("end")
        if not start_str or not end_str:
            return []

        # Find first non-empty sensor array to get length
        arr_len = 0
        for s in sensors:
            arr = inner.get(s, [])
            if arr:
                arr_len = len(arr)
                break

        if arr_len < 2:
            return []

        # Parse ISO timestamps to UTC unix
        t_start = datetime.fromisoformat(start_str.replace("Z", "+00:00")).timestamp()
        t_end   = datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp()
        step    = (t_end - t_start) / (arr_len - 1)
        times_unix = [t_start + (i * step) for i in range(arr_len)]

    records = []
    for i, ts in enumerate(times_unix):
        if ts is None:
            continue
        rec = {"recorded_at": datetime.fromtimestamp(ts, tz=timezone.utc)}
        has_value = False
        for sensor in sensors:
            arr = inner.get(sensor, [])
            val = arr[i] if i < len(arr) else None
            rec[sensor] = val
            if val is not None:
                has_value = True
        if has_value:
            records.append(rec)
    return records


# ── Core sync modes ───────────────────────────────────────────────────────────
def sync_device_recent(db, device: dict, hours: int = 48) -> int:
    """Fetch only the most recent N hours. Used for --recent / daily cron."""
    device_id   = device["id"]
    device_name = device["segment"]["name"]
    log.info("─── Recent sync: %s (%s) [last %dh] ───", device_name, device_id, hours)

    with db.cursor() as cur:
        upsert_device(cur, device)
    db.commit()

    now       = datetime.now(tz=timezone.utc)
    start_ts  = int((now - timedelta(hours=hours)).timestamp())
    end_ts    = int(now.timestamp())

    total_inserted = 0
    cursor         = None
    page           = 0

    while True:
        params = {"limit": SAMPLES_PER_PAGE, "start": start_ts, "end": end_ts}
        if cursor:
            params["cursor"] = cursor

        data       = api_get(f"/devices/{device_id}/samples", params)
        records    = decode_samples_response(data)
        new_cursor = data.get("cursor")

        if records:
            with db.cursor() as cur:
                n = insert_samples(cur, device_id, records)
                total_inserted += n
            db.commit()
            log.info("  Page %3d: %4d points, %4d new → total new %d",
                     page + 1, len(records), n, total_inserted)
        else:
            log.info("  Page %3d: empty — done", page + 1)
            break

        if not new_cursor or new_cursor == cursor:
            break

        cursor  = new_cursor
        page   += 1
        time.sleep(2)  # polite

    log.info("  Recent sync done. %d new rows inserted.", total_inserted)
    return total_inserted


def sync_device_range(db, device: dict, start_dt: datetime, end_dt: datetime) -> int:
    """Fetch a specific [start_dt, end_dt) window directly — used for targeted gap-fill
    so we don't have to cursor-crawl through months of already-synced data just to
    reach a known hole in the timeline."""
    device_id   = device["id"]
    device_name = device["segment"]["name"]
    log.info("─── Gap-fill sync: %s (%s) [%s → %s] ───", device_name, device_id, start_dt, end_dt)

    with db.cursor() as cur:
        upsert_device(cur, device)
        cur.execute(
            "SELECT MAX(recorded_at) FROM climate.airthings_samples WHERE device_id=%s AND recorded_at >= %s AND recorded_at < %s",
            (device_id, start_dt, end_dt)
        )
        existing_max = cur.fetchone()[0]
    db.commit()

    if existing_max:
        # Auto-resume: skip past data already inserted for this range so re-running
        # after a rate-limit stop doesn't re-fetch pages we already have.
        resume_dt = existing_max
        log.info("  Auto-resuming range from existing max recorded_at %s (was asked to start %s)",
                  resume_dt, start_dt)
        start_dt = resume_dt

    start_ts = int(start_dt.timestamp())
    end_ts   = int(end_dt.timestamp())

    total_inserted = 0
    cursor         = None
    page           = 0

    while True:
        params = {"limit": SAMPLES_PER_PAGE, "start": start_ts, "end": end_ts}
        if cursor:
            params["cursor"] = cursor

        try:
            data = api_get(f"/devices/{device_id}/samples", params)
        except RateLimitExhausted as e:
            log.warning("  %s — stopping gap-fill early, re-run same command to auto-resume from last inserted point.", e)
            break

        records    = decode_samples_response(data)
        new_cursor = data.get("cursor")

        if records:
            with db.cursor() as cur:
                n = insert_samples(cur, device_id, records)
                total_inserted += n
            db.commit()
            log.info("  Page %3d: %4d points, %4d new → total new %d",
                     page + 1, len(records), n, total_inserted)
        else:
            log.info("  Page %3d: empty — done", page + 1)
            break

        if not new_cursor or new_cursor == cursor:
            log.info("  No more pages in range.")
            break

        cursor  = new_cursor
        page   += 1
        time.sleep(2)  # gap-fill windows are small enough to be less rate-limit-hungry

    log.info("  Gap-fill done. %d new rows inserted for %s.", total_inserted, device_name)
    return total_inserted


def sync_device_full(db, device: dict, force_full: bool = False) -> int:
    """
    Full/incremental sync — resume from last cursor.
    Fetches in chronological order (oldest→newest) using the cursor chain.
    """
    device_id   = device["id"]
    device_name = device["segment"]["name"]
    log.info("─── Full sync: %s (%s) ───", device_name, device_id)

    with db.cursor() as cur:
        upsert_device(cur, device)
        state   = get_sync_state(cur, device_id)
        rows_db = state.get("total_rows", 0)
    db.commit()

    if force_full or not state:
        cursor = None
        log.info("  Starting from beginning (no prior state / --full)")
    else:
        cursor = state.get("cursor")
        log.info("  Resuming from cursor (last sync %s, %d rows recorded)",
                 state.get("last_sync_at"), rows_db)

    total_inserted = rows_db if not force_full else 0
    page           = 0

    while True:
        if page >= MAX_PAGES_PER_RUN:
            log.info("  Reached page budget (%d) for this run — saving progress, will resume next run.",
                      MAX_PAGES_PER_RUN)
            break

        params = {"limit": SAMPLES_PER_PAGE}
        if cursor:
            params["cursor"] = cursor

        try:
            data = api_get(f"/devices/{device_id}/samples", params)
        except RateLimitExhausted as e:
            log.warning("  %s — stopping this device's run early, progress already saved up to last successful page.", e)
            break

        records    = decode_samples_response(data)
        new_cursor = data.get("cursor")

        if records:
            with db.cursor() as cur:
                n = insert_samples(cur, device_id, records)
                total_inserted += n
                save_sync_state(cur, device_id, new_cursor, total_inserted)
            db.commit()
            log.info("  Page %3d: %4d points fetched, %4d new → total %d",
                     page + 1, len(records), n, total_inserted)
        else:
            log.info("  Page %3d: empty — done", page + 1)
            # Clear cursor so next run starts from latest
            with db.cursor() as cur:
                save_sync_state(cur, device_id, None, total_inserted)
            db.commit()
            break

        if not new_cursor or new_cursor == cursor:
            log.info("  No more pages.")
            with db.cursor() as cur:
                save_sync_state(cur, device_id, None, total_inserted)
            db.commit()
            break

        cursor  = new_cursor
        page   += 1
        time.sleep(5.0)  # polite — increased to 5s to avoid heavy 429s during backfill

    log.info("  Done (this run). %d total rows recorded for this device.", total_inserted)
    return total_inserted


def show_status(db):
    with db.cursor() as cur:
        cur.execute("""
            SELECT d.device_id, d.segment_name, d.location_name,
                   s.last_sync_at, s.total_rows,
                   MIN(sp.recorded_at) AS oldest,
                   MAX(sp.recorded_at) AS newest
            FROM climate.airthings_devices d
            LEFT JOIN climate.airthings_sync_state s ON s.device_id = d.device_id
            LEFT JOIN climate.airthings_samples sp ON sp.device_id = d.device_id
            GROUP BY d.device_id, d.segment_name, d.location_name,
                     s.last_sync_at, s.total_rows
            ORDER BY d.segment_name
        """)
        rows = cur.fetchall()
    print(f"\n{'Device':<30} {'Location':<20} {'Rows':>8}  {'Oldest':<22} {'Newest':<22} Last Sync")
    print("-" * 120)
    for r in rows:
        print(f"  {str(r[1]):<30} {str(r[2]):<20} {str(r[4] or 0):>8}"
              f"  {str(r[5] or '—'):<22} {str(r[6] or '—'):<22} {r[3] or '—'}")
    print()


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Airthings → PostgreSQL sync")
    parser.add_argument("--full",   action="store_true", help="Force full re-download")
    parser.add_argument("--recent", action="store_true", help="Only last 48h (for daily cron)")
    parser.add_argument("--hours",  type=int, default=48, help="Hours window for --recent (default 48)")
    parser.add_argument("--status", action="store_true", help="Show sync state and exit")
    parser.add_argument("--device", help="Sync only this device ID")
    parser.add_argument("--gap-start", help="ISO date/time (UTC) to start a targeted gap-fill, e.g. 2026-01-03")
    parser.add_argument("--gap-end", help="ISO date/time (UTC) to end a targeted gap-fill, e.g. 2026-04-29")
    args = parser.parse_args()

    init_auth()

    db = get_db()

    if args.status:
        show_status(db)
        db.close()
        return

    if args.gap_start or args.gap_end:
        if not (args.gap_start and args.gap_end and args.device):
            log.error("--gap-start/--gap-end require --device and both dates")
            db.close()
            return
        log.info("Fetching device list …")
        devices_data = api_get("/devices")
        devices = [d for d in devices_data.get("devices", []) if d["id"] == args.device]
        if not devices:
            log.error("Device %s not found", args.device)
            db.close()
            return
        start_dt = datetime.fromisoformat(args.gap_start).replace(tzinfo=timezone.utc)
        end_dt   = datetime.fromisoformat(args.gap_end).replace(tzinfo=timezone.utc)
        total = sync_device_range(db, devices[0], start_dt, end_dt)
        db.close()
        log.info("═══ Gap-fill complete. %d rows ═══", total)
        return

    log.info("Fetching device list …")
    devices_data = api_get("/devices")
    devices      = devices_data.get("devices", [])
    log.info("Found %d device(s)", len(devices))

    # Skip excluded device(s) unless explicitly requested with --device.
    # Set EXCLUDED_DEVICE_IDS via env var (comma-separated) if you want to skip any.
    EXCLUDED_DEVICE_IDS = {
        x.strip() for x in os.environ.get("AIRTHINGS_EXCLUDED_DEVICES", "").split(",") if x.strip()
    }
    if not args.device:
        before = len(devices)
        devices = [d for d in devices if d["id"] not in EXCLUDED_DEVICE_IDS]
        if len(devices) != before:
            log.info("Skipping excluded device(s): %s", EXCLUDED_DEVICE_IDS)

    if args.device:
        devices = [d for d in devices if d["id"] == args.device]
        if not devices:
            log.error("Device %s not found", args.device)
            db.close()
            return

    # For backfill runs, prioritize the device that has waited longest since its last
    # successful sync — prevents one slow/rate-limited device (e.g. Newton's) from
    # perpetually starving the others of a turn.
    if not args.recent:
        def last_sync_key(dev):
            with db.cursor() as cur:
                state = get_sync_state(cur, dev["id"])
            ts = state.get("last_sync_at")
            return ts or datetime.min.replace(tzinfo=timezone.utc)
        devices = sorted(devices, key=last_sync_key)
        log.info("Backfill device order (oldest last_sync_at first): %s",
                 [d["segment"]["name"] for d in devices])

    grand_total = 0
    for device in devices:
        try:
            if args.recent:
                grand_total += sync_device_recent(db, device, hours=args.hours)
            else:
                grand_total += sync_device_full(db, device, force_full=args.full)
        except Exception as e:
            log.error("Device %s (%s) failed, continuing with next device: %s",
                       device["segment"]["name"], device["id"], e)
            continue

    db.close()
    log.info("═══ Sync complete. Grand total: %d rows ═══", grand_total)


if __name__ == "__main__":
    main()

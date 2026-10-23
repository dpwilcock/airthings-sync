-- Airthings PostgreSQL Schema
-- Run: psql -U postgres -d postgres -f schema.sql

CREATE TABLE IF NOT EXISTS airthings_devices (
    device_id       TEXT PRIMARY KEY,
    device_type     TEXT,
    product_name    TEXT,
    segment_id      TEXT,
    segment_name    TEXT,
    location_id     TEXT,
    location_name   TEXT,
    sensors         TEXT[],
    segment_started TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS airthings_samples (
    id                    BIGSERIAL PRIMARY KEY,
    device_id             TEXT NOT NULL REFERENCES airthings_devices(device_id),
    recorded_at           TIMESTAMPTZ NOT NULL,
    radon_short_term_avg  REAL,   -- Bq/m³
    temp                  REAL,   -- °C
    humidity              REAL,   -- %RH
    pressure              REAL,   -- hPa
    co2                   REAL,   -- ppm
    voc                   REAL,   -- ppb
    pm1                   REAL,   -- µg/m³
    pm25                  REAL,   -- µg/m³
    UNIQUE (device_id, recorded_at)
);

CREATE INDEX IF NOT EXISTS idx_airthings_samples_device_time
    ON airthings_samples (device_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_airthings_samples_recorded_at
    ON airthings_samples (recorded_at DESC);

-- Track ingestion state per device
CREATE TABLE IF NOT EXISTS airthings_sync_state (
    device_id    TEXT PRIMARY KEY REFERENCES airthings_devices(device_id),
    last_cursor  TEXT,
    last_sync_at TIMESTAMPTZ,
    total_rows   BIGINT DEFAULT 0
);

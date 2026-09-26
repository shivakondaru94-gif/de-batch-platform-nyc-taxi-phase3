-- Gold-layer serving schema. This is the contract with the (out-of-scope)
-- machine learning application: it reads ml.features_demand_hourly and
-- nothing else, so the internals of the lake can change freely.

CREATE SCHEMA IF NOT EXISTS ml;
CREATE SCHEMA IF NOT EXISTS governance;

-- ---------------------------------------------------------------- features --
-- One row per pickup zone per hour: the aggregate the quarterly model trains on.
CREATE TABLE IF NOT EXISTS ml.features_demand_hourly (
    pickup_zone_id      INTEGER      NOT NULL,
    window_start        TIMESTAMP    NOT NULL,
    trip_count          BIGINT       NOT NULL,
    avg_trip_distance   NUMERIC(10,3),
    avg_fare_amount     NUMERIC(10,2),
    avg_duration_min    NUMERIC(10,2),
    p90_duration_min    NUMERIC(10,2),
    hour_of_day         SMALLINT     NOT NULL,
    day_of_week         SMALLINT     NOT NULL,
    is_weekend          BOOLEAN      NOT NULL,
    batch_id            TEXT         NOT NULL,
    generated_at        TIMESTAMP    NOT NULL DEFAULT now(),
    PRIMARY KEY (pickup_zone_id, window_start)
);

CREATE INDEX IF NOT EXISTS idx_features_window
    ON ml.features_demand_hourly (window_start);

-- -------------------------------------------------------------- governance --
-- Lineage: which source files produced which batch, and when. Makes every
-- gold row traceable back to a specific raw object in the bronze bucket.
CREATE TABLE IF NOT EXISTS governance.batch_lineage (
    batch_id        TEXT         PRIMARY KEY,
    source_uri      TEXT         NOT NULL,
    layer           TEXT         NOT NULL,
    rows_in         BIGINT,
    rows_out        BIGINT,
    rows_quarantined BIGINT,
    started_at      TIMESTAMP    NOT NULL,
    finished_at     TIMESTAMP,
    status          TEXT         NOT NULL
);

-- Data quality results, one row per expectation per batch.
CREATE TABLE IF NOT EXISTS governance.quality_checks (
    batch_id        TEXT         NOT NULL,
    expectation     TEXT         NOT NULL,
    observed        TEXT,
    passed          BOOLEAN      NOT NULL,
    checked_at      TIMESTAMP    NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, expectation)
);

-- Read-only role for the ML application: it can never mutate the feature store.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ml_reader') THEN
        CREATE ROLE ml_reader LOGIN PASSWORD 'change-me-reader';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA ml TO ml_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ml TO ml_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml GRANT SELECT ON TABLES TO ml_reader;

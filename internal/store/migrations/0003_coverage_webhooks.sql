-- Integrator coverage alerts: one row per registered threshold. `alerted` is
-- true while coverage is below min_bps, so one crossing produces one alert;
-- it is cleared when coverage recovers, re-arming the registration.
CREATE TABLE IF NOT EXISTS webhooks (
    id          BIGSERIAL PRIMARY KEY,
    url         TEXT NOT NULL,
    sac         TEXT NOT NULL,
    min_bps     INTEGER NOT NULL CHECK (min_bps BETWEEN 1 AND 10000),
    min_tier    SMALLINT NOT NULL CHECK (min_tier BETWEEN 1 AND 3),
    alerted     BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Delivery log: one row per alert, carrying the exact payload that is signed
-- and the retry state. A delivery is finished when delivered_at is set or
-- attempts reaches the worker's cap (5).
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id              BIGSERIAL PRIMARY KEY,
    webhook_id      BIGINT NOT NULL REFERENCES webhooks (id) ON DELETE CASCADE,
    payload         JSONB NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ,
    last_status     INTEGER,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS webhook_deliveries_pending
    ON webhook_deliveries (next_attempt_at)
    WHERE delivered_at IS NULL;

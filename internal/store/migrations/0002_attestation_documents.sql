-- Attestation-of-reserve documents listed by issuers in stellar.toml.
-- One row per (asset, hash): the table is a history of document versions, so
-- a replaced document is kept and its first_seen_at is when the new version
-- appeared. The current document is the row with the newest last_seen_at.
CREATE TABLE IF NOT EXISTS attestation_documents (
    sac           TEXT NOT NULL,
    url           TEXT NOT NULL,
    hash          TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sac, hash)
);

CREATE INDEX IF NOT EXISTS attestation_documents_sac_last_seen
    ON attestation_documents (sac, last_seen_at DESC);

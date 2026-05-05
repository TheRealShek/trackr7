-- ==========================================================================
-- REFERENCE ONLY — not embedded, not executed by trackr7.
-- Copy, adapt, and run in your own migration tooling.
--
-- trackr7 does not validate column types at runtime.
-- If your actual types diverge from this reference, behavior is undefined.
-- ==========================================================================

-- locations
-- trackr7 writes: uuid, entity_id, entity_type, lat, lng, ts (writer)
-- trackr7 reads:  (none from DB — cache reads from Kafka)
CREATE TABLE locations (
    uuid        TEXT    PRIMARY KEY,   -- client-generated, TEXT not UUID type
    entity_id   TEXT    NOT NULL,
    entity_type TEXT    NOT NULL,
    lat         FLOAT8  NOT NULL,
    lng         FLOAT8  NOT NULL,
    ts          BIGINT  NOT NULL       -- server UTC ms, display-only
);

CREATE INDEX idx_locations_entity_id ON locations USING btree (entity_id);
CREATE INDEX idx_locations_ts        ON locations USING brin  (ts);

-- api_keys
-- trackr7 reads:  key_id, key_hash, entity_type, revoked (auth cache)
-- trackr7 writes: key_id, key_hash, entity_type, revoked, created_at (admin)
CREATE TABLE api_keys (
    key_id      UUID    PRIMARY KEY,
    key_hash    TEXT    NOT NULL,
    entity_type TEXT    NOT NULL,
    revoked     BOOLEAN DEFAULT false,
    created_at  BIGINT  NOT NULL
);

-- audit_log
-- trackr7 writes: key_id, action, ts (admin)
-- No primary key — append-only log, never queried by row.
CREATE TABLE audit_log (
    key_id  UUID,
    action  TEXT,       -- created | revoked
    ts      BIGINT
);

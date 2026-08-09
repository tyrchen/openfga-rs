PRAGMA foreign_keys = ON;

CREATE TABLE openfga_schema_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL
) STRICT;

INSERT INTO openfga_schema_metadata (singleton, schema_version)
VALUES (1, 202608080001);

CREATE TABLE openfga_change_allocator (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    last_change_id TEXT COLLATE BINARY
) STRICT;

INSERT INTO openfga_change_allocator (singleton, last_change_id) VALUES (1, NULL);

CREATE TABLE stores (
    id TEXT COLLATE BINARY PRIMARY KEY,
    name TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    deleted_at_ms INTEGER
) STRICT;

CREATE INDEX stores_active_id_idx ON stores (id) WHERE deleted_at_ms IS NULL;

CREATE TABLE authorization_models (
    store_id TEXT COLLATE BINARY NOT NULL,
    model_id TEXT COLLATE BINARY NOT NULL,
    schema_version TEXT COLLATE BINARY NOT NULL,
    compiler_format_version INTEGER NOT NULL,
    source_fingerprint BLOB NOT NULL CHECK (length(source_fingerprint) = 32),
    source_payload BLOB NOT NULL CHECK (length(source_payload) <= 16777216),
    written_at_ms INTEGER NOT NULL,
    PRIMARY KEY (store_id, model_id)
) STRICT;

CREATE INDEX authorization_models_latest_idx
    ON authorization_models (store_id, model_id DESC);

CREATE TABLE assertions (
    store_id TEXT COLLATE BINARY NOT NULL,
    model_id TEXT COLLATE BINARY NOT NULL,
    assertions_payload BLOB NOT NULL CHECK (length(assertions_payload) <= 8388608),
    written_at_ms INTEGER NOT NULL,
    PRIMARY KEY (store_id, model_id),
    FOREIGN KEY (store_id, model_id)
        REFERENCES authorization_models(store_id, model_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE tuples (
    store_id TEXT COLLATE BINARY NOT NULL,
    object_type TEXT COLLATE BINARY NOT NULL,
    object_id TEXT COLLATE BINARY NOT NULL,
    relation TEXT COLLATE BINARY NOT NULL,
    subject_kind INTEGER NOT NULL CHECK (subject_kind BETWEEN 0 AND 2),
    subject_type TEXT COLLATE BINARY NOT NULL,
    subject_id TEXT COLLATE BINARY NOT NULL,
    subject_relation TEXT COLLATE BINARY NOT NULL DEFAULT '',
    condition_name TEXT COLLATE BINARY,
    condition_context BLOB,
    tuple_payload BLOB NOT NULL CHECK (length(tuple_payload) <= 2097152),
    inserted_at_ms INTEGER NOT NULL,
    PRIMARY KEY (
        store_id, object_type, object_id, relation, subject_kind,
        subject_type, subject_id, subject_relation
    ),
    CHECK ((condition_name IS NULL) = (condition_context IS NULL)),
    CHECK (condition_context IS NULL OR length(condition_context) <= 2097152),
    CHECK (
        (subject_kind = 0 AND subject_relation = '' AND subject_id <> '*') OR
        (subject_kind = 1 AND subject_relation <> '' AND subject_id <> '*') OR
        (subject_kind = 2 AND subject_relation = '' AND subject_id = '*')
    )
) STRICT;

CREATE INDEX tuples_forward_idx ON tuples (
    store_id, object_type, object_id, relation, subject_kind,
    subject_type, subject_id, subject_relation
);

CREATE INDEX tuples_reverse_idx ON tuples (
    store_id, subject_kind, subject_type, subject_id, subject_relation,
    object_type, relation, object_id
);

CREATE INDEX tuples_userset_idx ON tuples (
    store_id, object_type, object_id, relation,
    subject_type, subject_relation, subject_id
) WHERE subject_kind = 1;

CREATE TABLE tuple_changes (
    store_id TEXT COLLATE BINARY NOT NULL,
    change_id TEXT COLLATE BINARY NOT NULL,
    object_type TEXT COLLATE BINARY NOT NULL,
    object_id TEXT COLLATE BINARY NOT NULL,
    relation TEXT COLLATE BINARY NOT NULL,
    subject_kind INTEGER NOT NULL CHECK (subject_kind BETWEEN 0 AND 2),
    subject_type TEXT COLLATE BINARY NOT NULL,
    subject_id TEXT COLLATE BINARY NOT NULL,
    subject_relation TEXT COLLATE BINARY NOT NULL,
    condition_name TEXT COLLATE BINARY,
    condition_context BLOB,
    tuple_payload BLOB NOT NULL CHECK (length(tuple_payload) <= 2097152),
    operation INTEGER NOT NULL CHECK (operation IN (0, 1)),
    changed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (store_id, change_id),
    CHECK ((condition_name IS NULL) = (condition_context IS NULL)),
    CHECK (condition_context IS NULL OR length(condition_context) <= 2097152)
) STRICT;

CREATE INDEX tuple_changes_object_type_idx
    ON tuple_changes (store_id, object_type, change_id);

CREATE INDEX tuple_changes_time_idx
    ON tuple_changes (store_id, changed_at_ms, change_id);

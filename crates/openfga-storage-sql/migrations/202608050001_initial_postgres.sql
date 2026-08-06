CREATE TABLE openfga_schema_metadata (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    schema_version BIGINT NOT NULL
);

INSERT INTO openfga_schema_metadata (schema_version) VALUES (202608050001);

CREATE SEQUENCE openfga_change_sequence AS BIGINT NO CYCLE;

CREATE TABLE openfga_change_allocator (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    last_change_id VARCHAR(26) COLLATE "C"
);

INSERT INTO openfga_change_allocator (last_change_id) VALUES (NULL);

CREATE TABLE stores (
    id VARCHAR(26) COLLATE "C" PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE INDEX stores_active_id_idx
    ON stores (id COLLATE "C") WHERE deleted_at IS NULL;

CREATE TABLE authorization_models (
    store_id VARCHAR(26) COLLATE "C" NOT NULL,
    model_id VARCHAR(26) COLLATE "C" NOT NULL,
    schema_version VARCHAR(16) NOT NULL,
    compiler_format_version INTEGER NOT NULL,
    source_fingerprint BYTEA NOT NULL,
    source_payload BYTEA NOT NULL,
    written_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (store_id, model_id),
    CHECK (octet_length(source_fingerprint) = 32),
    CHECK (octet_length(source_payload) <= 16777216)
);

CREATE INDEX authorization_models_latest_idx
    ON authorization_models (store_id, model_id COLLATE "C" DESC);

CREATE TABLE assertions (
    store_id VARCHAR(26) COLLATE "C" NOT NULL,
    model_id VARCHAR(26) COLLATE "C" NOT NULL,
    assertions_payload BYTEA NOT NULL,
    written_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (store_id, model_id),
    FOREIGN KEY (store_id, model_id)
        REFERENCES authorization_models(store_id, model_id) ON DELETE CASCADE,
    CHECK (octet_length(assertions_payload) <= 8388608)
);

CREATE TABLE tuples (
    store_id VARCHAR(26) COLLATE "C" NOT NULL,
    object_type VARCHAR(254) COLLATE "C" NOT NULL,
    object_id VARCHAR(512) COLLATE "C" NOT NULL,
    relation VARCHAR(50) COLLATE "C" NOT NULL,
    subject_kind SMALLINT NOT NULL CHECK (subject_kind BETWEEN 0 AND 2),
    subject_type VARCHAR(254) COLLATE "C" NOT NULL,
    subject_id VARCHAR(512) COLLATE "C" NOT NULL,
    subject_relation VARCHAR(50) COLLATE "C" NOT NULL DEFAULT '',
    condition_name VARCHAR(50) COLLATE "C",
    condition_context BYTEA,
    tuple_payload BYTEA NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
        store_id,
        object_type,
        object_id,
        relation,
        subject_kind,
        subject_type,
        subject_id,
        subject_relation
    ),
    CHECK ((condition_name IS NULL) = (condition_context IS NULL)),
    CHECK (condition_context IS NULL OR octet_length(condition_context) <= 2097152),
    CHECK (octet_length(tuple_payload) <= 2097152),
    CHECK (
        (subject_kind = 0 AND subject_relation = '' AND subject_id <> '*') OR
        (subject_kind = 1 AND subject_relation <> '' AND subject_id <> '*') OR
        (subject_kind = 2 AND subject_relation = '' AND subject_id = '*')
    )
);

CREATE INDEX tuples_forward_idx ON tuples (
    store_id,
    object_type COLLATE "C",
    object_id COLLATE "C",
    relation COLLATE "C",
    subject_kind,
    subject_type COLLATE "C",
    subject_id COLLATE "C",
    subject_relation COLLATE "C"
);

CREATE INDEX tuples_reverse_idx ON tuples (
    store_id,
    subject_kind,
    subject_type COLLATE "C",
    subject_id COLLATE "C",
    subject_relation COLLATE "C",
    object_type COLLATE "C",
    relation COLLATE "C",
    object_id COLLATE "C"
);

CREATE INDEX tuples_userset_idx ON tuples (
    store_id,
    object_type COLLATE "C",
    object_id COLLATE "C",
    relation COLLATE "C",
    subject_type COLLATE "C",
    subject_relation COLLATE "C",
    subject_id COLLATE "C"
) WHERE subject_kind = 1;

CREATE TABLE tuple_changes (
    store_id VARCHAR(26) COLLATE "C" NOT NULL,
    change_id VARCHAR(26) COLLATE "C" NOT NULL,
    object_type VARCHAR(254) COLLATE "C" NOT NULL,
    object_id VARCHAR(512) COLLATE "C" NOT NULL,
    relation VARCHAR(50) COLLATE "C" NOT NULL,
    subject_kind SMALLINT NOT NULL CHECK (subject_kind BETWEEN 0 AND 2),
    subject_type VARCHAR(254) COLLATE "C" NOT NULL,
    subject_id VARCHAR(512) COLLATE "C" NOT NULL,
    subject_relation VARCHAR(50) COLLATE "C" NOT NULL,
    condition_name VARCHAR(50) COLLATE "C",
    condition_context BYTEA,
    tuple_payload BYTEA NOT NULL,
    operation SMALLINT NOT NULL CHECK (operation IN (0, 1)),
    changed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (store_id, change_id),
    CHECK ((condition_name IS NULL) = (condition_context IS NULL)),
    CHECK (condition_context IS NULL OR octet_length(condition_context) <= 2097152),
    CHECK (octet_length(tuple_payload) <= 2097152)
);

CREATE INDEX tuple_changes_object_type_idx
    ON tuple_changes (store_id, object_type COLLATE "C", change_id COLLATE "C");

CREATE INDEX tuple_changes_time_idx
    ON tuple_changes (store_id, changed_at, change_id COLLATE "C");

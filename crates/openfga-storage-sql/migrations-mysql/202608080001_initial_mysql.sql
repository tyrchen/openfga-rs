CREATE TABLE openfga_schema_metadata (
    singleton BOOLEAN PRIMARY KEY,
    schema_version BIGINT NOT NULL,
    CHECK (singleton = TRUE)
);

INSERT INTO openfga_schema_metadata (singleton, schema_version)
VALUES (TRUE, 202608080001);

CREATE TABLE openfga_change_allocator (
    singleton BOOLEAN PRIMARY KEY,
    last_change_id VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NULL,
    CHECK (singleton = TRUE)
);

INSERT INTO openfga_change_allocator (singleton, last_change_id) VALUES (TRUE, NULL);

CREATE TABLE stores (
    id VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    name VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    deleted_at_ms BIGINT NULL
);

CREATE INDEX stores_active_id_idx ON stores (deleted_at_ms, id);

CREATE TABLE authorization_models (
    store_id VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    model_id VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    schema_version VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    compiler_format_version INTEGER NOT NULL,
    source_fingerprint BINARY(32) NOT NULL,
    source_payload MEDIUMBLOB NOT NULL,
    written_at_ms BIGINT NOT NULL,
    PRIMARY KEY (store_id, model_id)
);

CREATE INDEX authorization_models_latest_idx
    ON authorization_models (store_id, model_id DESC);

CREATE TABLE assertions (
    store_id VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    model_id VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    assertions_payload MEDIUMBLOB NOT NULL,
    written_at_ms BIGINT NOT NULL,
    PRIMARY KEY (store_id, model_id),
    FOREIGN KEY (store_id, model_id)
        REFERENCES authorization_models(store_id, model_id) ON DELETE CASCADE
);

CREATE TABLE tuples (
    store_id VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    object_type VARCHAR(254) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    object_id VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    relation VARCHAR(50) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    subject_kind SMALLINT NOT NULL,
    subject_type VARCHAR(254) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    subject_id VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    subject_relation VARCHAR(50) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    condition_name VARCHAR(50) CHARACTER SET ascii COLLATE ascii_bin NULL,
    condition_context MEDIUMBLOB NULL,
    tuple_payload MEDIUMBLOB NOT NULL,
    inserted_at_ms BIGINT NOT NULL,
    PRIMARY KEY (
        store_id, object_type, object_id, relation, subject_kind,
        subject_type, subject_id, subject_relation
    ),
    CHECK (subject_kind BETWEEN 0 AND 2),
    CHECK ((condition_name IS NULL) = (condition_context IS NULL)),
    CHECK (
        (subject_kind = 0 AND subject_relation = '' AND subject_id <> '*') OR
        (subject_kind = 1 AND subject_relation <> '' AND subject_id <> '*') OR
        (subject_kind = 2 AND subject_relation = '' AND subject_id = '*')
    )
);

CREATE INDEX tuples_forward_idx ON tuples (
    store_id, object_type, object_id, relation, subject_kind,
    subject_type, subject_id, subject_relation
);

CREATE INDEX tuples_reverse_idx ON tuples (
    store_id, subject_kind, subject_type, subject_id, subject_relation,
    object_type, relation, object_id
);

CREATE INDEX tuples_userset_idx ON tuples (
    store_id, subject_kind, object_type, object_id, relation,
    subject_type, subject_relation, subject_id
);

CREATE TABLE tuple_changes (
    store_id VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    change_id VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    object_type VARCHAR(254) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    object_id VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    relation VARCHAR(50) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    subject_kind SMALLINT NOT NULL,
    subject_type VARCHAR(254) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    subject_id VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    subject_relation VARCHAR(50) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    condition_name VARCHAR(50) CHARACTER SET ascii COLLATE ascii_bin NULL,
    condition_context MEDIUMBLOB NULL,
    tuple_payload MEDIUMBLOB NOT NULL,
    operation SMALLINT NOT NULL,
    changed_at_ms BIGINT NOT NULL,
    PRIMARY KEY (store_id, change_id),
    CHECK (subject_kind BETWEEN 0 AND 2),
    CHECK (operation IN (0, 1)),
    CHECK ((condition_name IS NULL) = (condition_context IS NULL))
);

CREATE INDEX tuple_changes_object_type_idx
    ON tuple_changes (store_id, object_type, change_id);

CREATE INDEX tuple_changes_time_idx
    ON tuple_changes (store_id, changed_at_ms, change_id);

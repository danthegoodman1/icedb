
-- +migrate Up
CREATE TABLE files (
    enabled BOOLEAN NOT NULL,
    namespace TEXT NOT NULL,
    partition TEXT NOT NULL,
    name TEXT NOT NULL,

    bytes INT8 NOT NULL,
    rows INT8 NOT NULL,
    columns TEXT[] NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY(enabled, namespace, partition, name)
)
;

-- +migrate Down
DROP TABLE files;
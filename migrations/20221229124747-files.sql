
-- +migrate Up
CREATE TABLE files (
    namespace TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    path TEXT NOT NULL,

    bytes INT8 NOT NULL,
    rows INT8 NOT NULL,
    columns TEXT[] NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY(namespace, enabled, path)
)
;

-- +migrate Down
DROP TABLE files;
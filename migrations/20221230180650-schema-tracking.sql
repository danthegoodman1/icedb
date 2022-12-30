
-- +migrate Up
CREATE TABLE columns (
    namespace TEXT NOT NULL,
    col TEXT NOT NULL,
    type TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(namespace, col)
)
;

-- +migrate Down
DROP TABLE columns;
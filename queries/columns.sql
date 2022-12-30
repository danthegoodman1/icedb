-- name: TryInsertColumn :exec
INSERT INTO columns (namespace, col, type)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING
;

-- name: GetColumns :many
SELECT *
FROM columns
WHERE namespace = $1
;

-- name: InsertColumns :exec
INSERT INTO columns (namespace, col, type)
SELECT @namespace,
       UNNEST(@col_names::TEXT[]) as col,
       UNNEST(@col_types::TEXT[]) AS type
ON CONFLICT DO NOTHING
;

-- name: ListNamespaces :many
-- Yes I know...
SELECT DISTINCT namespace
FROM columns
;
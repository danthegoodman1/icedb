-- name: GetAllEnabledFiles :many
SELECT path
FROM files
WHERE namespace = $1
AND enabled = true
;

-- name: InsertFile :exec
INSERT INTO files (
    namespace,
    enabled,
    path,
    bytes,
    rows
) VALUES (
    @namespace,
    @enabled,
    @path,
    @bytes,
    @rows
)
;
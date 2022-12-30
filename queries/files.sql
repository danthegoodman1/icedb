-- name: GetAllEnabledFiles :many
SELECT partition, name
FROM files
WHERE namespace = $1
AND enabled = true
;

-- name: InsertFile :exec
INSERT INTO files (
    namespace,
    enabled,
    bytes,
    rows,
    columns,
    partition,
    name
) VALUES (
    @namespace,
    @enabled,
    @bytes,
    @rows,
    @columns,
    @partition,
    @name
)
;

-- name: SetFileStates :exec
UPDATE files
SET enabled = @enabled,
updated_at = NOW()
WHERE enabled != @enabled
AND namespace = @namespace
AND partition = @partition
AND name = ANY(@names::TEXT[])
;

-- name: SelectFilesForMerging :many
SELECT *
FROM files
WHERE namespace = @namespace
AND enabled = true
AND bytes <= @max_bytes
LIMIT @max_files
;
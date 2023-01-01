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
    name,
    json_schema
) VALUES (
    @namespace,
    @enabled,
    @bytes,
    @rows,
    @columns,
    @partition,
    @name,
    @json_schema
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
WITH eligible_files AS (
    SELECT count(*) as cnt, partition
    FROM files
    WHERE namespace = @namespace
    AND enabled = true
    AND bytes <= @max_bytes
    GROUP BY partition
    HAVING count(*) >= 2
    ORDER BY partition ASC
    LIMIT 1
)
SELECT *
FROM files
JOIN eligible_files ON eligible_files.partition = files.partition
WHERE files.namespace = @namespace
AND files.enabled = true
AND files.partition = eligible_files.partition
AND files.bytes <= @max_bytes
LIMIT @max_files
;
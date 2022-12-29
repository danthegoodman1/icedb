package http_server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/danthegoodman1/gojsonutils"
	"github.com/danthegoodman1/icedb/crdb"
	"github.com/danthegoodman1/icedb/parquet_accumulator"
	"github.com/danthegoodman1/icedb/query"
	"github.com/danthegoodman1/icedb/s3"
	"github.com/danthegoodman1/icedb/utils"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/xitongsys/parquet-go/writer"
	"net/http"
	"time"
)

type (
	InsertStats struct {
		NumRows      int64
		BytesWritten int64
		TimeNS       int64
	}
)

var (
	ErrNotFlatMap = errors.New("not a flat map")
)

func (s *HTTPServer) InsertHandler(c *CustomContext) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), time.Second*60)
	defer cancel()

	//logger := zerolog.Ctx(ctx)

	start := time.Now()
	// Get namespace to write to

	// Extract rows (flattened) and columns from format (JSON, NDJSON)
	ndJSONScanner := bufio.NewScanner(c.Request().Body)
	defer c.Request().Body.Close()

	psa := parquet_accumulator.NewParquetAccumulator()
	var rawFlatRows []map[string]any

	for ndJSONScanner.Scan() {
		var raw any
		err := json.Unmarshal([]byte(ndJSONScanner.Text()), &raw)
		if err != nil {
			return fmt.Errorf("error in json.Unmarshal: %w", err)
		}
		jsonMap, ok := raw.(map[string]any)
		if !ok {
			return c.String(http.StatusBadRequest, "line was not JSON")
		}
		flat, err := gojsonutils.Flatten(jsonMap, nil)
		if err != nil {
			return c.InternalError(err, "error flattening JSON map")
		}
		flatMap, ok := flat.(map[string]any)
		if !ok {
			return c.InternalError(ErrNotFlatMap, fmt.Sprintf("got a non flat map: %+v", flat))
		}

		rawFlatRows = append(rawFlatRows, flatMap)
		psa.WriteRow(flatMap)
	}

	if len(rawFlatRows) == 0 {
		return c.String(http.StatusBadRequest, "no rows found")
	}

	// Generate parquet schema from columns
	parquetSchema, err := psa.GetSchemaString()
	if err != nil {
		return c.InternalError(err, "error in GetSchemaString")
	}

	// Convert rows to a parquet file
	var b bytes.Buffer
	pw, err := writer.NewJSONWriterFromWriter(parquetSchema, &b, 4)
	if err != nil {
		return c.InternalError(err, "error in NewJSONWriterFromWriter")
	}

	for _, row := range rawFlatRows {
		rowBytes, err := json.Marshal(row)
		if err != nil {
			return c.InternalError(err, "error in json.Marshal of flat row")
		}
		err = pw.Write(rowBytes)
		if err != nil {
			return c.InternalError(err, "error in pw.Write")
		}
	}
	err = pw.WriteStop()
	if err != nil {
		return c.InternalError(err, "error in pw.WriteStop")
	}

	byteLen := b.Len()

	// Write parquet file to S3
	// TODO: Based on partition scheme
	fileName := fmt.Sprintf("p/%s.parquet", utils.GenKSortedID(""))
	_, err = s3.WriteBytesToS3(ctx, fileName, &b, nil)
	if err != nil {
		return c.InternalError(err, "error uploading to s3")
	}

	// Insert file metadata
	err = utils.ReliableExec(ctx, crdb.PGPool, time.Second*10, func(ctx context.Context, conn *pgxpool.Conn) error {
		q := query.New(conn)
		return q.InsertFile(ctx, query.InsertFileParams{
			Namespace: "tempns",
			Enabled:   true,
			Path:      fileName,
			Bytes:     int64(byteLen),
			Rows:      int64(len(rawFlatRows)),
			Columns:   psa.GetColumns(),
		})
	})
	end := time.Since(start)

	// Respond with metrics
	stats := InsertStats{
		NumRows:      int64(len(rawFlatRows)),
		BytesWritten: int64(byteLen),
		TimeNS:       end.Nanoseconds(),
	}

	return c.JSON(http.StatusAccepted, stats)
}

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
	"github.com/danthegoodman1/icedb/partitioner"
	"github.com/danthegoodman1/icedb/query"
	"github.com/danthegoodman1/icedb/s3_helper"
	"github.com/danthegoodman1/icedb/utils"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/xitongsys/parquet-go/writer"
	"net/http"
	"strings"
	"time"
)

type (
	InsertReqBody struct {
		Namespace string
		// Line-delimited JSON (NDJSON)
		RowsString *string
		// Array of JSON
		Rows        []*map[string]any
		Partitioner []partitioner.PartitionPlan
	}

	InsertStats struct {
		NumRows      int64
		NumFiles     int64
		BytesWritten int64
		TimeMS       int64
	}

	PartitionData struct {
		Accumulator parquet_accumulator.ParquetSchemaAccumulator
		Rows        []map[string]any
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

	var reqBody InsertReqBody
	if err := ValidateRequest(c, &reqBody); err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	// Get namespace to write to

	// Extract rows (flattened) and columns from format (JSON, NDJSON)
	defer c.Request().Body.Close()

	parts := make(map[string]*PartitionData)

	if reqBody.RowsString != nil {
		ndJSONScanner := bufio.NewScanner(strings.NewReader(*reqBody.RowsString))
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

			// Determine partition and write row to that
			part, err := partitioner.GetRowPartition(flatMap, reqBody.Partitioner)
			if err != nil {
				// TODO: Check for user errors
				return c.InternalError(err, "error getting partition for row")
			}

			if _, exists := parts[part]; !exists {
				parts[part] = &PartitionData{
					Accumulator: parquet_accumulator.NewParquetAccumulator(),
				}
			}

			p := parts[part]

			p.Rows = append(p.Rows, flatMap)
			p.Accumulator.WriteRow(flatMap)
		}
	} else if reqBody.Rows != nil {
		for _, row := range reqBody.Rows {
			flat, err := gojsonutils.Flatten(*row, nil)
			if err != nil {
				return c.InternalError(err, "error flattening JSON map")
			}
			fmt.Printf("%+v\n", flat)
			flatMap, ok := flat.(map[string]any)
			if !ok {
				return c.InternalError(ErrNotFlatMap, fmt.Sprintf("got a non flat map: %+v", flat))
			}

			// Determine partition and write row to that
			part, err := partitioner.GetRowPartition(flatMap, reqBody.Partitioner)
			if err != nil {
				// TODO: Check for user errors
				return c.InternalError(err, "error getting partition for row")
			}

			if _, exists := parts[part]; !exists {
				parts[part] = &PartitionData{
					Accumulator: parquet_accumulator.NewParquetAccumulator(),
				}
			}

			p := parts[part]

			p.Rows = append(p.Rows, flatMap)
			p.Accumulator.WriteRow(flatMap)
		}
	}

	if len(parts) == 0 {
		return c.String(http.StatusBadRequest, "no rows found")
	}

	var totalBytes int64 = 0
	var numRows int64 = 0

	for partID, partData := range parts {
		// Generate parquet schema from columns
		parquetSchema, err := partData.Accumulator.GetSchemaString()
		if err != nil {
			return c.InternalError(err, "error in GetSchemaString")
		}

		// Convert rows to a parquet file
		var b bytes.Buffer
		pw, err := writer.NewJSONWriterFromWriter(parquetSchema, &b, 4)
		if err != nil {
			return c.InternalError(err, "error in NewJSONWriterFromWriter")
		}

		for _, row := range partData.Rows {
			rowBytes, err := json.Marshal(row)
			if err != nil {
				return c.InternalError(err, "error in json.Marshal of flat row")
			}
			err = pw.Write(rowBytes)
			if err != nil {
				return c.InternalError(err, fmt.Sprintf("error in pw.Write for row %+v", string(rowBytes)))
			}
			numRows++
		}
		err = pw.WriteStop()
		if err != nil {
			return c.InternalError(err, "error in pw.WriteStop")
		}

		byteLen := b.Len()
		totalBytes += int64(byteLen)

		// Write parquet file to S3
		fileName := fmt.Sprintf("%s.parquet", utils.GenKSortedID(""))
		_, err = s3_helper.WriteBytesToS3(ctx, fmt.Sprintf("ns=%s/%s/%s", reqBody.Namespace, partID, fileName), &b, nil)
		if err != nil {
			return c.InternalError(err, "error uploading to s3")
		}

		err = utils.ReliableExec(ctx, crdb.PGPool, time.Second*10, func(ctx context.Context, conn *pgxpool.Conn) error {
			q := query.New(conn)
			// Insert file metadata
			err := q.InsertFile(ctx, query.InsertFileParams{
				Namespace: reqBody.Namespace,
				Enabled:   true,
				Partition: partID,
				Name:      fileName,
				Bytes:     int64(byteLen),
				Rows:      int64(len(partData.Rows)),
				Columns:   partData.Accumulator.GetColumnNames(),
			})
			if err != nil {
				return fmt.Errorf("error in InsertFile: %w", err)
			}

			// Try to insert columns
			err = q.InsertColumns(ctx, query.InsertColumnsParams{
				Namespace: reqBody.Namespace,
				ColNames:  partData.Accumulator.GetColumnNames(),
				ColTypes:  partData.Accumulator.GetColumnTypes(),
			})
			if err != nil {
				return fmt.Errorf("error in InsertColumns: %w", err)
			}
			return nil
		})
		if err != nil {
			return c.InternalError(err, "error inserting file")
		}

	}

	end := time.Since(start)

	// Respond with metrics
	stats := InsertStats{
		NumRows:      numRows,
		BytesWritten: totalBytes,
		NumFiles:     int64(len(parts)),
		TimeMS:       end.Milliseconds(),
	}

	return c.JSON(http.StatusAccepted, stats)
}

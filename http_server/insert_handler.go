package http_server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/danthegoodman1/gojsonutils"
	"github.com/danthegoodman1/icedb/parquet_accumulator"
	"github.com/rs/zerolog"
	"github.com/xitongsys/parquet-go/writer"
	"net/http"
	"time"
)

var (
	ErrNotFlatMap = errors.New("not a flat map")
)

func (s *HTTPServer) InsertHandler(c *CustomContext) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), time.Second*60)
	defer cancel()

	logger := zerolog.Ctx(ctx)

	// Get namespace to write to

	// Extract rows (flattened) and columns from format (JSON, NDJSON)
	ndJSONScanner := bufio.NewScanner(c.Request().Body)
	defer c.Request().Body.Close()

	psa := parquet_accumulator.NewParquetAccumulator()
	var rawFlatRows []map[string]any

	for ndJSONScanner.Scan() {
		var raw any
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

		psa.WriteRow(flatMap)
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
		bytes, err := json.Marshal(row)
		if err != nil {
			return c.InternalError(err, "error in json.Marshal of flat row")
		}
		err = pw.Write(bytes)
		if err != nil {
			return c.InternalError(err, "error in pw.Write")
		}
	}
	err = pw.WriteStop()
	if err != nil {
		return c.InternalError(err, "error in pw.WriteStop")
	}

	// Write parquet file to S3

	// Insert file metadata

	// Respond with metrics

	return c.String(http.StatusAccepted, "")
}

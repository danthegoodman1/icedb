package datastore

import (
	"context"
	"github.com/danthegoodman1/GoAPITemplate/gologger"
)

var (
	logger = gologger.NewLogger()
)

type (
	temp string

	DataStore interface {
		// GetColumnFile creates a reader for an entire column file
		GetColumnFile(ctx context.Context, table, partID, column string) (temp, error)
		// GetColumnFileWithOffsets creates a reader for a column file with byte offsets
		GetColumnFileWithOffsets(ctx context.Context, table, partID, column string, start, end int64) (temp, error)

		// WriteColumnFile creates a writer for a column file
		WriteColumnFile(ctx context.Context, table, partID string, t temp) error

		Shutdown(ctx context.Context) error
	}
)

package datastore

import (
	"context"
	"github.com/danthegoodman1/GoAPITemplate/gologger"
	"io"
)

var (
	logger = gologger.NewLogger()
)

type (
	temp string

	DataStore interface {
		// GetColumnFile creates a reader for an entire column file.
		// The caller is responsible for closing.
		GetColumnFile(ctx context.Context, table, partID, column string) (io.Reader, error)
		// GetColumnFileWithOffsets creates a reader for a column file with byte offsets
		//GetColumnFileWithOffsets(ctx context.Context, table, partID, column string, start, end int64) ([]io.Reader, error)

		// WriteColumnFile creates a writer for a column file. The caller is responsible for closing.
		WriteColumnFile(ctx context.Context, table, partID, column string) (io.Writer, error)

		Shutdown(ctx context.Context) error
	}
)

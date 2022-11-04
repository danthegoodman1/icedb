package datastore

import "github.com/danthegoodman1/GoAPITemplate/gologger"

var (
	logger = gologger.NewLogger()
)

type (
	temp string

	DataStore interface {
		// GetColumnFile creates a reader for an entire column file
		GetColumnFile(table, partID, column string) (temp, error)
		// GetColumnFileWithOffsets creates a reader for a column file with byte offsets
		GetColumnFileWithOffsets(table, partID, column string) (temp, error)

		// WriteColumnFile creates a writer for a column file
		WriteColumnFile(table, partID string, t temp) error
	}
)

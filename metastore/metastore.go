package metastore

import (
	"github.com/danthegoodman1/GoAPITemplate/gologger"
	"github.com/danthegoodman1/icedb/part"
	"github.com/danthegoodman1/icedb/table"
)

var (
	logger = gologger.NewLogger()
)

type (
	MetaStore interface {
		// GetTableSchema fetches the table schema for a given table
		GetTableSchema() (table.TableSchema, error)

		// ListParts lists all parts for a table
		ListParts(table string) ([]part.Part, error)
		ListPartsInPartitions(table string, partIDs []string) ([]part.Part, error)

		ListGranulesInPart(table, partID string) ([]part.Granule, error)

		// WritePart writes the part index and marks for each column contained in the part
		WritePart(table string, p part.Part) error
	}
)

package metastore

import (
	"github.com/danthegoodman1/GoAPITemplate/gologger"
	"github.com/danthegoodman1/icedb/part"
	"time"
)

var (
	logger = gologger.NewLogger()
)

type (
	MetaStore interface {
		// GetTableSchema fetches the table schema for a given table
		GetTableSchema() (TableSchema, error)

		// ListParts lists all parts for a table
		ListParts(table string) ([]part.Part, error)
		ListPartsInPartitions(table string, partIDs []string) ([]part.Part, error)

		// WritePart writes the part index and marks for each column contained in the part
		WritePart(table string, p part.Part) error
		CreateTableSchema(
			tableName string,
			partKeyColNames,
			partKeyColTypes,
			orderingKeyColNames,
			orderingKeyColTypes []string,
		) error
		// InsertPart creates a new part index and mark files
		CreatePart(table string, p part.Part, colMarks []part.ColumnMark) error
	}

	TableSchema struct {
		ID   string
		Name string

		PartitionKeyColNames []string
		PartitionKeyColTypes []string

		OrderingKeyColNames []string
		OrderingKeyColTypes []string

		CreatedAt time.Time
		UpdatedAt time.Time
	}
)

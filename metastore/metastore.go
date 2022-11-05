package metastore

import (
	"context"
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
		GetTableSchema(ctx context.Context) (TableSchema, error)

		// ListParts lists all parts for a table
		ListParts(ctx context.Context, table string) ([]part.Part, error)
		ListPartsInPartitions(ctx context.Context, table string, partitions []string) ([]part.Part, error)

		CreateTableSchema(
			ctx context.Context,
			tableName string,
			partKeyColNames,
			partKeyColTypes,
			orderingKeyColNames,
			orderingKeyColTypes []string,
		) error
		// CreatePart creates a new part index and mark files
		CreatePart(ctx context.Context, table string, p part.Part, colMarks []part.ColumnMark) error

		Shutdown(ctx context.Context) error
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

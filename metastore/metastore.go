package metastore

import (
	"context"
	"errors"
	"github.com/danthegoodman1/icedb/part"
	"time"
)

var (
	ErrTableNotFound = errors.New("table not found")
)

type (
	MetaStore interface {
		// GetTableSchema fetches the table schema for a given table
		GetTableSchema(ctx context.Context, table string) (TableSchema, error)

		// ListParts lists all parts for a table, only returns `Alive` parts
		ListParts(ctx context.Context, table string) ([]part.Part, error)
		// ListPartsInPartitions lists all parts for a given set of partitions, only returns `Alive` parts
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

		// TableKey formats the name of the table to a key
		TableKey(tableName string) string
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

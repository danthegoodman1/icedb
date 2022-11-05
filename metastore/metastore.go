package metastore

import (
	"context"
	"errors"
	"github.com/danthegoodman1/icedb/part"
	"time"
)

var (
	ErrTableNotFound = errors.New("table not found")

	LT    FilterOperator = "lt"
	LTE   FilterOperator = "lte"
	GT    FilterOperator = "gt"
	GTE   FilterOperator = "gte"
	IN    FilterOperator = "in"
	NOTIN FilterOperator = "notin"
)

type (
	MetaStore interface {
		// GetTableSchema fetches the table schema for a given table
		GetTableSchema(ctx context.Context, table string) (TableSchema, error)

		// ListParts lists all parts for a table, only returns `Alive` parts
		ListParts(ctx context.Context, table string, filters ...FilterOption) ([]part.Part, error)

		CreateTableSchema(
			ctx context.Context,
			tableName string,
			partKeyOps,
			orderingKeyOps []part.KeyOp,
			granularity uint64,
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

		// The max number of rows in a granule
		Granularity uint64

		PartKeyOps     []part.KeyOp
		OrderingKeyOps []part.KeyOp

		CreatedAt time.Time
		UpdatedAt time.Time
	}

	FilterOperator string

	FilterOption struct {
		Operator FilterOperator
		Val      any
	}
)

func WithFilterOption(operator FilterOperator, val any) FilterOption {
	return FilterOption{
		Operator: operator,
		Val:      val,
	}
}

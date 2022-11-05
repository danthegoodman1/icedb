package table

import "go/types"

type (
	TableSchema struct {
		ID   string
		Name string

		PrimaryKeyColNames []string
		PrimaryKeyColTypes []types.Type

		OrderingKeyColNames []string
		OrderingKeyColTypes []types.Type
	}

	Row struct {
		// The column number within the part
		Num int64

		// The list of column names, same order as ColVals
		ColNames []string
		// The lsit of column values, same order as ColNums
		ColVals []any
	}
)

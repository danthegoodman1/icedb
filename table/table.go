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
)

package main

import (
	"context"
	"fmt"
	"github.com/danthegoodman1/icedb/part"
)

// CreateTableSchema creates a new table
func (idb *IceDB) CreateTableSchema(
	ctx context.Context,
	tableName string,
	partKeyOps,
	orderingKeyOps []part.KeyOp,
	granularity uint64,
) error {
	err := idb.MetaStore.CreateTableSchema(ctx, tableName, partKeyOps, orderingKeyOps, granularity)
	if err != nil {
		return fmt.Errorf("error in idb.MetaStore.CreateTableSchema: %w", err)
	}

	return nil
}

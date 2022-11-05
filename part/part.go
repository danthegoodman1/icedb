package part

import "time"

type (
	Part struct {
		ID        string
		Alive     bool
		CreatedAt time.Time
		RowCount  int64
		// GranulePrimaryKeyVals is the ordered set of primary keys at the start of each granule
		GranulePrimaryKeyVals [][]string
	}

	ColumnMark struct {
		PartID string
		// GranuleIndexes represents the present granules of a part within a column
		GranuleIndexes []string
		// ByteOffsets is an ordered byte offset for the granule indexes
		ByteOffsets []int64
		ColumnName  string
		// RowCount is the number of rows within this column
		RowCount int64
	}
)

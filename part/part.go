package part

import "time"

type (
	Part struct {
		ID string
		// Alive is whether the part should be considered for queries
		Alive     bool
		CreatedAt time.Time
		RowCount  int64
		// GranulePrimaryKeyVals is the ordered set of primary keys at the start of each granule. The primary key index.
		GranulePrimaryKeyVals [][]string
	}

	ColumnMark struct {
		PartID string
		// GranuleIndex is the index of the granule within the part
		GranuleIndex int64
		// ByteOffsets is an ordered byte offset for the granule indexes
		ByteOffsets int64
		ColumnName  string
		// RowCount is the number of rows within this granule
		RowCount int64
		// RowStart is the row number that is the first row in the granule, mapped to the
		// primary key rows. As such row numbers may be skipped within a column if not all rows
		// in the part contain this column.
		RowStart int64
	}

	// PartKeyOp is an operation on a column for a partition key
	PartKeyOp struct {
		ColNames   string
		PreOpType  string
		PostOpType string
	}
)

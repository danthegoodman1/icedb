package part

import "time"

type (
	Part struct {
		ID        string
		Alive     bool
		CreatedAt time.Time
	}

	Granule struct {
		ID               string
		PartitionKeyVals []any
	}

	ColumnMark struct {
		PartID     string
		GranuleIDs []string
	}
)

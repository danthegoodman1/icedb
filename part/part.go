package part

import "time"

type (
	Part struct {
		ID        string
		Alive     bool
		CreatedAt time.Time
	}

	Granule struct {
		ID             string
		PrimaryKeyVals []any
	}

	ColumnMark struct {
		PartID     string
		GranuleIDs []string
	}
)

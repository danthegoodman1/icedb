package table

type (
	Row struct {
		// The column number within the part
		Num     int64
		Granule int64

		// The list of column names, same order as ColVals
		ColNames []string
		// The lsit of column values, same order as ColNums
		ColVals []any
	}
)

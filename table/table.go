package table

type (
	Row struct {
		// The list of column names, same order as ColVals
		ColNames []string
		// The lsit of column values, same order as ColNums
		ColVals []any
	}
)

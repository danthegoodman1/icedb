package utils

import "os"

var (
	CRDB_DSN = os.Getenv("CRDB_DSN")
)

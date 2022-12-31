package utils

import "os"

var (
	CRDB_DSN = os.Getenv("CRDB_DSN")

	AWS_ACCESS_KEY_ID     = os.Getenv("AWS_ACCESS_KEY_ID")
	AWS_SECRET_ACCESS_KEY = os.Getenv("AWS_SECRET_ACCESS_KEY")
	AWS_DEFAULT_REGION    = GetEnvOrDefault("AWS_DEFAULT_REGION", "us-east-1")

	S3_BUCKET_NAME = os.Getenv("S3_BUCKET_NAME")
	S3_ENDPOINT    = os.Getenv("S3_ENDPOINT")

	USERNAME = os.Getenv("USERNAME")
	PASSWORD = os.Getenv("PASSWORD")
)

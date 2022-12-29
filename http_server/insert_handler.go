package http_server

import "net/http"

func (s *HTTPServer) InsertHandler(c *CustomContext) error {

	// Get namespace to write to

	// Extract rows (flattened) and columns from format (JSON, NDJSON)

	// Generate parquet schema from columns

	// Convert rows to a parquet file

	// Write parquet file to S3

	// Insert file metadata

	// Respond with metrics

	return c.String(http.StatusAccepted, "")
}

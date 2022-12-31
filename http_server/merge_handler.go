package http_server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/danthegoodman1/icedb/crdb"
	"github.com/danthegoodman1/icedb/parquet_accumulator"
	"github.com/danthegoodman1/icedb/query"
	"github.com/danthegoodman1/icedb/s3_helper"
	"github.com/danthegoodman1/icedb/utils"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
	s3_pq "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"net/http"
	"reflect"
	"time"
)

type (
	MergeReqBody struct {
		Namespace string
		// The partition path, minus the leading `ns={Namespace}/`.
		//
		// Ex: `year=2022/month=12/day=30`
		Partition *string
		// The max file size in bytes that will be considered for merging.
		//
		// Default 1GB.
		MaxPreMergeFileBytes *int64
		// The max file size after merge, controls how many files can be merged.
		//
		// Default 5GB.
		MaxPostMergeFileBytes *int64
		// Max number of files to merge at once.
		//
		// Default 4.
		MaxMergeFiles *int32
		// How many seconds before the merge will time out.
		//
		// Default `60`.
		MaxRuntimeSec *int64
	}

	MergeStats struct {
		FilesMerged int64
		RowsMerged  int64
		// The size of the file after merging
		PostMergeBytes int64
		TimeMS         int64
		// The partition path, minus the leading `ns={Namespace}/`.
		//
		// Ex: `year=2022/month=12/day=30`
		PartitionsMerged []string
	}
)

func (s *HTTPServer) MergeHandler(c *CustomContext) error {

	var reqBody MergeReqBody
	if err := ValidateRequest(c, &reqBody); err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	ctx, cancel := context.WithTimeout(c.Request().Context(), time.Second*time.Duration(utils.Deref(reqBody.MaxRuntimeSec, 60)))
	defer cancel()

	logger := zerolog.Ctx(ctx)

	logger.Debug().Msg("running merge handler")

	var res MergeStats

	start := time.Now()

	// Get the files we want to merge
	var enabledFilesToMerge []query.SelectFilesForMergingRow
	st := time.Now()
	err := utils.ReliableExec(ctx, crdb.PGPool, time.Second*time.Duration(utils.Deref(reqBody.MaxRuntimeSec, 60)), func(ctx context.Context, conn *pgxpool.Conn) (err error) {
		q := query.New(conn)

		enabledFilesToMerge, err = q.SelectFilesForMerging(ctx, query.SelectFilesForMergingParams{
			Namespace: reqBody.Namespace,
			MaxBytes:  utils.Deref(reqBody.MaxPreMergeFileBytes, 1_000_000_000),
			MaxFiles:  utils.Deref(reqBody.MaxMergeFiles, 4),
		})

		if err != nil {
			return fmt.Errorf("error in SelectFilesForMerging: %w", err)
		}

		return
	})
	if err != nil {
		return c.InternalError(err, "error getting files for merging")
	}
	logger.Debug().Msgf("got files to merge in %s", time.Since(st))
	if len(enabledFilesToMerge) < 2 {
		logger.Debug().Msg("not enough files to merge")
		return c.NoContent(http.StatusNoContent)
	}

	// Start merging
	accumulator := parquet_accumulator.NewParquetAccumulator()
	var bMerged bytes.Buffer
	var flatRows []map[string]any

	conf := &aws.Config{
		Region:      aws.String(utils.AWS_DEFAULT_REGION),
		Credentials: credentials.NewEnvCredentials(),
	}
	if utils.S3_ENDPOINT != "" {
		conf.Endpoint = aws.String(utils.S3_ENDPOINT)
	}

	sess, err := session.NewSession(conf)
	if err != nil {
		return c.InternalError(err, "error making new aws session")
	}

	s3Client := s3.New(sess)

	// Get files to merge
	for _, file := range enabledFilesToMerge {
		st := time.Now()
		logger := logger.With().Str("fileName", file.Name).Str("partition", file.Partition).Logger()
		logger.Debug().Msg("reading file from S3")
		r, err := s3_pq.NewS3FileReaderWithParams(context.Background(), s3_pq.S3FileReaderParams{
			Bucket:   utils.S3_BUCKET_NAME,
			Key:      fmt.Sprintf("ns=%s/%s/%s", reqBody.Namespace, file.Partition, file.Name),
			S3Client: s3Client,
		})
		if err != nil {
			return c.InternalError(err, "error creating new s3 file reader")
		}

		pr, err := reader.NewParquetReader(r, nil, 4)
		if err != nil {
			return c.InternalError(err, "error creating parquet reader for file "+file.Name+" in namespace "+reqBody.Namespace)
		}
		logger.Debug().Msgf("got %d rows", pr.GetNumRows())
		res.RowsMerged += pr.GetNumRows()
		rows, err := pr.ReadByNumber(int(pr.GetNumRows()))
		if err != nil {
			return c.InternalError(err, "error reading rows for file "+file.Name+" in namespace "+reqBody.Namespace)
		}
		// Struct -> Map (not very efficient right now)
		for _, row := range rows {
			// row is a struct
			rowMap := make(map[string]any)
			v := reflect.ValueOf(row)
			typeOf := v.Type()
			for i := 0; i < v.NumField(); i++ {
				rowMap[typeOf.Field(i).Name] = v.Field(i).Interface()
			}
			flatRows = append(flatRows, rowMap)
			accumulator.WriteRow(rowMap)
		}
		res.FilesMerged++
		logger.Debug().Msgf("read file to merge in %s", time.Since(st))
	}

	st = time.Now()
	parquetSchema, err := accumulator.GetSchemaString()
	if err != nil {
		return c.InternalError(err, "error getting schema string")
	}
	pw, err := writer.NewJSONWriterFromWriter(parquetSchema, &bMerged, 4)
	if err != nil {
		return c.InternalError(err, "error creating new JSON writer")
	}

	for _, row := range flatRows {
		rowBytes, err := json.Marshal(row)
		if err != nil {
			return c.InternalError(err, "error in json.Marshal of flat row")
		}
		err = pw.Write(rowBytes)
		if err != nil {
			return c.InternalError(err, fmt.Sprintf("error in pw.Write for row %+v", string(rowBytes)))
		}
	}
	err = pw.WriteStop()
	if err != nil {
		return c.InternalError(err, "error in pw.WriteStop")
	}

	res.PostMergeBytes = int64(bMerged.Len())

	// Write parquet file to S3
	fileName := fmt.Sprintf("%s.parquet", utils.GenKSortedID(""))
	_, err = s3_helper.WriteBytesToS3(ctx, fmt.Sprintf("ns=%s/%s/%s", reqBody.Namespace, enabledFilesToMerge[0].Partition, fileName), &bMerged, nil)
	if err != nil {
		return c.InternalError(err, "error uploading to s3")
	}

	// Update meta store
	err = utils.ReliableExecInTx(ctx, crdb.PGPool, time.Second*time.Duration(utils.Deref(reqBody.MaxRuntimeSec, 60)), func(ctx context.Context, conn pgx.Tx) error {
		q := query.New(conn)
		err := q.InsertFile(ctx, query.InsertFileParams{
			Namespace: reqBody.Namespace,
			Enabled:   true,
			Partition: enabledFilesToMerge[0].Partition,
			Name:      fileName,
			Bytes:     res.PostMergeBytes,
			Rows:      res.RowsMerged,
			Columns:   accumulator.GetColumnNames(),
		})
		if err != nil {
			return fmt.Errorf("error in InsertFile: %w", err)
		}

		logger.Debug().Msg("inserted new file")

		// Disable the old files
		var fileNames []string
		for _, file := range enabledFilesToMerge {
			fileNames = append(fileNames, file.Name)
		}

		err = q.SetFileStates(ctx, query.SetFileStatesParams{
			Enabled:   false,
			Namespace: reqBody.Namespace,
			Partition: enabledFilesToMerge[0].Partition,
			Names:     fileNames,
		})
		if err != nil {
			return fmt.Errorf("error in SetFileState: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("error updating meta store: %w", err)
	}

	logger.Debug().Interface("response", res).Msg("merged files")

	res.TimeMS = time.Since(start).Milliseconds()

	return c.JSON(http.StatusOK, res)
}

package s3

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/danthegoodman1/icedb/gologger"
	"github.com/danthegoodman1/icedb/utils"
	"github.com/rs/zerolog"
)

var (
	logger = gologger.NewLogger()
)

func WriteBytesToS3(ctx context.Context, fileName string, byteStream io.Reader, contentType *string) (*s3manager.UploadOutput, error) {

	ctx = logger.WithContext(ctx)
	logger := zerolog.Ctx(ctx)

	s3Config := &aws.Config{
		Region:      aws.String(utils.AWS_DEFAULT_REGION),
		Credentials: credentials.NewEnvCredentials(),
	}
	if utils.S3_ENDPOINT != "" {
		s3Config.Endpoint = aws.String(utils.S3_ENDPOINT)
	}

	s3Session, err := session.NewSession(s3Config)
	if err != nil {
		return nil, fmt.Errorf("error making new session: %w", err)
	}

	uploader := s3manager.NewUploader(s3Session)

	input := &s3manager.UploadInput{
		Bucket:      aws.String(utils.S3_BUCKET_NAME),
		Key:         aws.String(fileName),
		Body:        byteStream,
		ContentType: contentType,
	}

	s := time.Now()
	output, err := uploader.UploadWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("error uploading to s3: %w", err)
	}

	d := time.Since(s)
	logger.Debug().Str("fileName", fileName).Int64("durationNS", d.Nanoseconds()).Str("durationHuman", d.String()).Msg("uploaded file to s3")

	return output, nil
}

// Package s3client wraps aws-sdk-go-v2 S3 access for din: blob PutObject for
// split externalization and ListObjectsV2 for lake backfill discovery. It
// implements objstore.Store and split.ObjectStore.
package s3client

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/din/internal/objstore"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// putObjectTimeout bounds a single blob externalization on the ingest hot path,
// so a stalled S3 endpoint degrades to a fast error (and WAL redelivery) instead
// of pinning the request goroutine on an established-but-hung connection past the
// SDK's generous defaults. Retries are bounded by maxRetryAttempts.
const (
	putObjectTimeout = 30 * time.Second
	maxRetryAttempts = 3
)

// Config holds the S3 connection settings.
type Config struct {
	// Bucket is the bucket all client operations target.
	Bucket string
	// Region is the AWS region; empty falls back to the default chain.
	Region string
	// AccessKeyID and SecretAccessKey configure static credentials; leave
	// both empty to use the default AWS credential chain.
	AccessKeyID     string
	SecretAccessKey string
	// Endpoint overrides the S3 endpoint (MinIO, localstack); when set,
	// path-style addressing is enabled.
	Endpoint string
}

// ObjectInfo describes a listed object. Alias of objstore.ObjectInfo so
// s3client and fsstore satisfy one objstore.Store interface.
type ObjectInfo = objstore.ObjectInfo

// api is the subset of the AWS S3 client the wrapper uses.
type api interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// Client is a bucket-bound S3 wrapper.
type Client struct {
	s3     api
	bucket string
}

// New builds a Client from cfg, loading the AWS config chain.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}

	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}
	// Make the retry contract explicit rather than relying on the SDK default.
	opts = append(opts, awsconfig.WithRetryMaxAttempts(maxRetryAttempts))

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})

	return &Client{s3: s3Client, bucket: cfg.Bucket}, nil
}

// PutObject uploads body under key. Implements split.ObjectStore.
func (c *Client) PutObject(ctx context.Context, key string, body []byte) error {
	// Bound the upload so a stalled S3 endpoint degrades to a fast error (and WAL
	// redelivery) instead of pinning the ingest request goroutine on a hung conn.
	ctx, cancel := context.WithTimeout(ctx, putObjectTimeout)
	defer cancel()
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("put object %s/%s: %w", c.bucket, key, err)
	}
	return nil
}

// ListObjectsV2 lists all objects under prefix, following pagination.
func (c *Client) ListObjectsV2(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(c.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing objects under %s/%s: %w", c.bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			objects = append(objects, ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			})
		}
	}
	return objects, nil
}

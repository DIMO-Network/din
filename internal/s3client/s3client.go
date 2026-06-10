// Package s3client wraps aws-sdk-go-v2 S3 access for din. It implements
// split.ObjectStore for blob externalization and exposes the get/list/delete
// surface the parquet compactor needs (ported from
// DIMO-Network/parquet-processor).
package s3client

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const deleteBatchSize = 1000

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

// ObjectInfo describes a listed object.
type ObjectInfo struct {
	Key  string
	Size int64
}

// api is the subset of the AWS S3 client the wrapper uses.
type api interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
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

// GetObject downloads the object at key. maxSize > 0 rejects objects larger
// than maxSize bytes; maxSize <= 0 reads without bound.
func (c *Client) GetObject(ctx context.Context, key string, maxSize int64) ([]byte, error) {
	resp, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s/%s: %w", c.bucket, key, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if maxSize <= 0 {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read object %s/%s: %w", c.bucket, key, err)
		}
		return data, nil
	}

	if resp.ContentLength != nil && *resp.ContentLength > maxSize {
		return nil, fmt.Errorf("object %s/%s exceeds max size of %d bytes", c.bucket, key, maxSize)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSize+1))
	if err != nil {
		return nil, fmt.Errorf("read object %s/%s: %w", c.bucket, key, err)
	}
	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("object %s/%s exceeds max size of %d bytes", c.bucket, key, maxSize)
	}
	return data, nil
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

// DeleteObjects removes keys in batches of 1000 (the S3 API limit) and
// surfaces per-key delete failures as an error.
func (c *Client) DeleteObjects(ctx context.Context, keys []string) error {
	for i := 0; i < len(keys); i += deleteBatchSize {
		end := i + deleteBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		objects := make([]s3types.ObjectIdentifier, len(batch))
		for j, key := range batch {
			objects[j] = s3types.ObjectIdentifier{Key: aws.String(key)}
		}

		resp, err := c.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &s3types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("delete objects batch starting at %d: %w", i, err)
		}
		if len(resp.Errors) > 0 {
			first := resp.Errors[0]
			return fmt.Errorf("delete objects batch starting at %d: %d keys failed, first error: %s (key: %s)",
				i, len(resp.Errors), aws.ToString(first.Message), aws.ToString(first.Key))
		}
	}
	return nil
}

package s3client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAPI implements the api interface for unit tests; no real S3.
type fakeAPI struct {
	putInputs  []*s3.PutObjectInput
	putErr     error
	listPages  []*s3.ListObjectsV2Output
	listInputs []*s3.ListObjectsV2Input
	listErr    error
}

func (f *fakeAPI) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.putInputs = append(f.putInputs, in)
	if f.putErr != nil {
		return nil, f.putErr
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeAPI) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.listInputs = append(f.listInputs, in)
	if f.listErr != nil {
		return nil, f.listErr
	}
	page := f.listPages[0]
	f.listPages = f.listPages[1:]
	return page, nil
}

func newTestClient(api *fakeAPI) *Client {
	return &Client{s3: api, bucket: "test-bucket"}
}

func TestPutObject(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	c := newTestClient(api)

	body := []byte("hello blob")
	require.NoError(t, c.PutObject(context.Background(), "cloudevent/blobs/sub/2024/06/15/abc", body))

	require.Len(t, api.putInputs, 1)
	in := api.putInputs[0]
	assert.Equal(t, "test-bucket", aws.ToString(in.Bucket))
	assert.Equal(t, "cloudevent/blobs/sub/2024/06/15/abc", aws.ToString(in.Key))
	assert.Equal(t, "application/octet-stream", aws.ToString(in.ContentType))

	got, err := io.ReadAll(in.Body)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

// TestPutObject_SSEKMS proves a configured KMSKeyID turns every blob PutObject
// into an SSE-KMS request with a Bucket Key. The blob path is not covered by
// DuckLake's ENCRYPTED, so this is its only at-rest layer.
func TestPutObject_SSEKMS(t *testing.T) {
	t.Parallel()
	const keyARN = "arn:aws:kms:us-east-2:1:key/abc"
	api := &fakeAPI{}
	c := &Client{s3: api, bucket: "test-bucket", kmsKeyID: keyARN}

	require.NoError(t, c.PutObject(context.Background(), "blob", []byte("payload")))
	require.Len(t, api.putInputs, 1)
	in := api.putInputs[0]
	assert.Equal(t, s3types.ServerSideEncryptionAwsKms, in.ServerSideEncryption)
	assert.Equal(t, keyARN, aws.ToString(in.SSEKMSKeyId))
	assert.True(t, aws.ToBool(in.BucketKeyEnabled), "Bucket Key must be enabled to amortize KMS calls")
}

// TestPutObject_NoKMS leaves the SSE headers unset when no key is configured,
// so the bucket default (SSE-S3) applies instead of failing closed.
func TestPutObject_NoKMS(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	c := newTestClient(api)

	require.NoError(t, c.PutObject(context.Background(), "blob", []byte("payload")))
	require.Len(t, api.putInputs, 1)
	in := api.putInputs[0]
	assert.Equal(t, s3types.ServerSideEncryption(""), in.ServerSideEncryption)
	assert.Nil(t, in.SSEKMSKeyId)
	assert.Nil(t, in.BucketKeyEnabled)
}

func TestPutObject_ErrorWrapped(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{putErr: errors.New("denied")}
	c := newTestClient(api)

	err := c.PutObject(context.Background(), "k", []byte("v"))
	require.Error(t, err)
	assert.ErrorIs(t, err, api.putErr)
	assert.Contains(t, err.Error(), "test-bucket/k")
}

func TestListObjectsV2_Paginates(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{listPages: []*s3.ListObjectsV2Output{
		{
			Contents: []s3types.Object{
				{Key: aws.String("prefix/a.parquet"), Size: aws.Int64(10)},
				{Key: aws.String("prefix/b.parquet"), Size: aws.Int64(20)},
			},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("token-1"),
		},
		{
			Contents: []s3types.Object{
				{Key: aws.String("prefix/c.json"), Size: aws.Int64(30)},
			},
			IsTruncated: aws.Bool(false),
		},
	}}
	c := newTestClient(api)

	objects, err := c.ListObjectsV2(context.Background(), "prefix/")
	require.NoError(t, err)

	assert.Equal(t, []ObjectInfo{
		{Key: "prefix/a.parquet", Size: 10},
		{Key: "prefix/b.parquet", Size: 20},
		{Key: "prefix/c.json", Size: 30},
	}, objects)

	require.Len(t, api.listInputs, 2, "paginator must follow the continuation token")
	assert.Equal(t, "prefix/", aws.ToString(api.listInputs[0].Prefix))
	assert.Equal(t, "test-bucket", aws.ToString(api.listInputs[0].Bucket))
	assert.Equal(t, "token-1", aws.ToString(api.listInputs[1].ContinuationToken))
}

func TestListObjectsV2_ErrorWrapped(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{listErr: errors.New("throttled")}
	c := newTestClient(api)

	_, err := c.ListObjectsV2(context.Background(), "prefix/")
	require.Error(t, err)
	assert.ErrorIs(t, err, api.listErr)
}

func TestNew_RequiresBucket(t *testing.T) {
	t.Parallel()
	_, err := New(context.Background(), Config{})
	require.Error(t, err)
}

func TestPutObject_BodyIsReadable(t *testing.T) {
	t.Parallel()
	// Guard against accidental aliasing: the reader handed to the SDK must
	// produce exactly the bytes passed in.
	api := &fakeAPI{}
	c := newTestClient(api)

	body := bytes.Repeat([]byte{0xde, 0xad}, 512)
	require.NoError(t, c.PutObject(context.Background(), "bin", body))

	got, err := io.ReadAll(api.putInputs[0].Body)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

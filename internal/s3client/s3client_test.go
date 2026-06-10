package s3client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAPI implements the api interface for unit tests; no real S3.
type fakeAPI struct {
	putInputs    []*s3.PutObjectInput
	putErr       error
	getOutput    *s3.GetObjectOutput
	getErr       error
	listPages    []*s3.ListObjectsV2Output
	listInputs   []*s3.ListObjectsV2Input
	listErr      error
	deleteInputs []*s3.DeleteObjectsInput
	deleteOutput *s3.DeleteObjectsOutput
	deleteErr    error
}

func (f *fakeAPI) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.putInputs = append(f.putInputs, in)
	if f.putErr != nil {
		return nil, f.putErr
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeAPI) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.getOutput, nil
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

func (f *fakeAPI) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	f.deleteInputs = append(f.deleteInputs, in)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	if f.deleteOutput != nil {
		return f.deleteOutput, nil
	}
	return &s3.DeleteObjectsOutput{}, nil
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

func TestPutObject_ErrorWrapped(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{putErr: errors.New("denied")}
	c := newTestClient(api)

	err := c.PutObject(context.Background(), "k", []byte("v"))
	require.Error(t, err)
	assert.ErrorIs(t, err, api.putErr)
	assert.Contains(t, err.Error(), "test-bucket/k")
}

func TestGetObject(t *testing.T) {
	t.Parallel()
	payload := strings.Repeat("d", 100)

	t.Run("within max size", func(t *testing.T) {
		api := &fakeAPI{getOutput: &s3.GetObjectOutput{
			Body:          io.NopCloser(strings.NewReader(payload)),
			ContentLength: aws.Int64(int64(len(payload))),
		}}
		c := newTestClient(api)

		got, err := c.GetObject(context.Background(), "k", 200)
		require.NoError(t, err)
		assert.Equal(t, []byte(payload), got)
	})

	t.Run("no max size reads everything", func(t *testing.T) {
		api := &fakeAPI{getOutput: &s3.GetObjectOutput{
			Body: io.NopCloser(strings.NewReader(payload)),
		}}
		c := newTestClient(api)

		got, err := c.GetObject(context.Background(), "k", 0)
		require.NoError(t, err)
		assert.Equal(t, []byte(payload), got)
	})

	t.Run("content length over max size", func(t *testing.T) {
		api := &fakeAPI{getOutput: &s3.GetObjectOutput{
			Body:          io.NopCloser(strings.NewReader(payload)),
			ContentLength: aws.Int64(int64(len(payload))),
		}}
		c := newTestClient(api)

		_, err := c.GetObject(context.Background(), "k", 50)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds max size")
	})

	t.Run("body longer than advertised content length", func(t *testing.T) {
		api := &fakeAPI{getOutput: &s3.GetObjectOutput{
			Body:          io.NopCloser(strings.NewReader(payload)),
			ContentLength: aws.Int64(10), // lies
		}}
		c := newTestClient(api)

		_, err := c.GetObject(context.Background(), "k", 50)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds max size")
	})

	t.Run("get error wrapped", func(t *testing.T) {
		api := &fakeAPI{getErr: errors.New("no such key")}
		c := newTestClient(api)

		_, err := c.GetObject(context.Background(), "k", 0)
		require.Error(t, err)
		assert.ErrorIs(t, err, api.getErr)
	})
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

func TestDeleteObjects_Batches(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	c := newTestClient(api)

	keys := make([]string, 0, deleteBatchSize+5)
	for i := range deleteBatchSize + 5 {
		keys = append(keys, fmt.Sprintf("key-%04d", i))
	}

	require.NoError(t, c.DeleteObjects(context.Background(), keys))

	require.Len(t, api.deleteInputs, 2, "1005 keys must be split into two batches")
	assert.Len(t, api.deleteInputs[0].Delete.Objects, deleteBatchSize)
	assert.Len(t, api.deleteInputs[1].Delete.Objects, 5)
	assert.Equal(t, "key-0000", aws.ToString(api.deleteInputs[0].Delete.Objects[0].Key))
	assert.Equal(t, fmt.Sprintf("key-%04d", deleteBatchSize), aws.ToString(api.deleteInputs[1].Delete.Objects[0].Key))
	assert.True(t, aws.ToBool(api.deleteInputs[0].Delete.Quiet))
}

func TestDeleteObjects_Empty(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	c := newTestClient(api)

	require.NoError(t, c.DeleteObjects(context.Background(), nil))
	assert.Empty(t, api.deleteInputs, "no API call for an empty key list")
}

func TestDeleteObjects_PerKeyErrorsSurface(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{deleteOutput: &s3.DeleteObjectsOutput{
		Errors: []s3types.Error{
			{Key: aws.String("key-1"), Message: aws.String("access denied")},
			{Key: aws.String("key-2"), Message: aws.String("access denied")},
		},
	}}
	c := newTestClient(api)

	err := c.DeleteObjects(context.Background(), []string{"key-1", "key-2"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "2 keys failed")
	assert.Contains(t, err.Error(), "access denied")
	assert.Contains(t, err.Error(), "key-1")
}

func TestDeleteObjects_RequestErrorWrapped(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{deleteErr: errors.New("connection reset")}
	c := newTestClient(api)

	err := c.DeleteObjects(context.Background(), []string{"key-1"})
	require.Error(t, err)
	assert.ErrorIs(t, err, api.deleteErr)
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

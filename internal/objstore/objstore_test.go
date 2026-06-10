package objstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsLocalPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		bucket string
		local  bool
		root   string
	}{
		{"/data/pipeline", true, "/data/pipeline"},
		{"file:///data/pipeline", true, "/data/pipeline"},
		{"my-bucket", false, "my-bucket"},
		{"s3://my-bucket", false, "s3://my-bucket"},
		{"./relative", false, "./relative"},
		{"", false, ""},
	}
	for _, c := range cases {
		assert.Equal(t, c.local, IsLocalPath(c.bucket), "IsLocalPath(%q)", c.bucket)
		assert.Equal(t, c.root, LocalRoot(c.bucket), "LocalRoot(%q)", c.bucket)
	}
}

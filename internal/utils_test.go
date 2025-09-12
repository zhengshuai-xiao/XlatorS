package internal

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSplitDir(t *testing.T) {
	assert.Equal(t, []string{"a", "b", "c"}, SplitDir("a,b,c"))
	assert.Equal(t, []string{"a", "b"}, SplitDir("a:b")) // Assuming PathListSeparator is ':' on Unix
	assert.Equal(t, []string{"a"}, SplitDir("a"))
}

func TestRemovePassword(t *testing.T) {
	assert.Equal(t, "redis://user:****@host:6379", RemovePassword("redis://user:password@host:6379"))
	assert.Equal(t, "http://host/path", RemovePassword("http://host/path"))
	assert.Equal(t, "user:****@host", RemovePassword("user:pass@host"))
}

func TestGuessMimeType(t *testing.T) {
	assert.Equal(t, "image/jpeg", GuessMimeType("photo.jpg"))
	assert.Equal(t, "application/pdf", GuessMimeType("document.pdf"))
	assert.Equal(t, "application/octet-stream", GuessMimeType("filewithoutextension"))
	assert.Equal(t, "application/octet-stream", GuessMimeType("unknown.ext"))
}

func TestFormatBytes(t *testing.T) {
	assert.Equal(t, "1023 Bytes", FormatBytes(1023))
	assert.Equal(t, "1.00 KiB (1024 Bytes)", FormatBytes(1024))
	assert.Equal(t, "1.50 KiB (1536 Bytes)", FormatBytes(1536))
	assert.Equal(t, "1.00 MiB (1048576 Bytes)", FormatBytes(1024*1024))
	assert.Equal(t, "1.00 GiB (1073741824 Bytes)", FormatBytes(1024*1024*1024))
}

func TestDuration(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected time.Duration
	}{
		{"Seconds", "5s", 5 * time.Second},
		{"Minutes and Seconds", "1m30s", 90 * time.Second},
		{"Days and Hours", "1d2h", 26 * time.Hour},
		{"Float seconds", "1.5", 1500 * time.Millisecond},
		{"Empty string", "", 0},
		{"Invalid string", "abc", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, Duration(tc.input))
		})
	}
}

func TestStringContains(t *testing.T) {
	slice := []string{"apple", "banana", "cherry"}
	assert.True(t, StringContains(slice, "banana"))
	assert.False(t, StringContains(slice, "grape"))
}

func TestExists(t *testing.T) {
	// Create a temporary file
	tmpfile, err := os.CreateTemp("", "exists_test")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	assert.True(t, Exists(tmpfile.Name()))
	assert.False(t, Exists(tmpfile.Name()+".nonexistent"))
}

func TestWithTimeout(t *testing.T) {
	t.Run("Function completes in time", func(t *testing.T) {
		err := WithTimeout(func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}, 50*time.Millisecond)
		assert.NoError(t, err)
	})

	t.Run("Function times out", func(t *testing.T) {
		err := WithTimeout(func() error {
			time.Sleep(100 * time.Millisecond)
			return nil
		}, 20*time.Millisecond)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrFuncTimeout)
	})
}

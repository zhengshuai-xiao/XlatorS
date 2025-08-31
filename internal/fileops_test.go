package internal

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Name string
	ID   int
}

func TestFileSerialization(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test-serialize-*.bin")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	original := testStruct{Name: "Test", ID: 123}

	// Test SerializeToFile
	err = SerializeToFile(original, tmpfile)
	assert.NoError(t, err)

	// Reset file pointer for reading
	_, err = tmpfile.Seek(0, 0)
	assert.NoError(t, err)

	// Test DeserializeFromFile
	var deserialized testStruct
	err = DeserializeFromFile(tmpfile, &deserialized)
	assert.NoError(t, err)

	assert.Equal(t, original, deserialized)
	tmpfile.Close()
}

func TestWriteAll(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test-writeall-*.txt")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	content := []byte("this is a test content for WriteAll")
	n, err := WriteAll(tmpfile, content)
	assert.NoError(t, err)
	assert.Equal(t, len(content), n)

	// Read back and verify
	readContent, err := ioutil.ReadFile(tmpfile.Name())
	assert.NoError(t, err)
	assert.Equal(t, content, readContent)
}

func TestWriteReadCloserToFile(t *testing.T) {
	content := "this is a test for WriteReadCloserToFile"
	reader := ioutil.NopCloser(strings.NewReader(content))

	tmpfilePath := filepath.Join(os.TempDir(), "test-writerc.txt")
	defer os.Remove(tmpfilePath)

	n, err := WriteReadCloserToFile(reader, tmpfilePath)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(content)), n)

	// Read back and verify
	readContent, err := ioutil.ReadFile(tmpfilePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte(content), readContent)
}

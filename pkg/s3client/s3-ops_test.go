package S3client

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculateFileHashes(t *testing.T) {
	// Create a temporary file with some content
	content := []byte("hello world, this is a test file for hashing.")
	tmpfile, err := ioutil.TempFile("", "test-hash-*.txt")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name()) // clean up

	_, err = tmpfile.Write(content)
	assert.NoError(t, err)

	// Reset file pointer to the beginning before passing to the function
	_, err = tmpfile.Seek(0, 0)
	assert.NoError(t, err)

	// Calculate hashes using the function under test
	md5b64, sha256hex, err := calculateFileHashes(tmpfile)
	assert.NoError(t, err)

	// Calculate expected hashes manually
	expectedMD5 := md5.Sum(content)
	expectedMD5b64 := base64.StdEncoding.EncodeToString(expectedMD5[:])

	expectedSHA256 := sha256.Sum256(content)
	expectedSHA256hex := hex.EncodeToString(expectedSHA256[:])

	// Assert the results
	assert.Equal(t, expectedMD5b64, md5b64, "MD5 hash should match")
	assert.Equal(t, expectedSHA256hex, sha256hex, "SHA256 hash should match")
}

func TestCalculateFileHashes_EmptyFile(t *testing.T) {
	// Create an empty temporary file
	tmpfile, err := ioutil.TempFile("", "test-hash-empty-*.txt")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name()) // clean up

	md5b64, sha256hex, err := calculateFileHashes(tmpfile)
	assert.NoError(t, err)

	assert.Equal(t, "1B2M2Y8AsgTpgAmY7PhCfg==", md5b64, "MD5 hash for empty file should match")
	assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", sha256hex, "SHA256 hash for empty file should match")
}

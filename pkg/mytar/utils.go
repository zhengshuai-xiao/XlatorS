package mytar

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// TarFileHeader holds metadata for a single file within a tar archive.
// It includes the original tar header and its offset within the combined DATA object.
type TarFileHeader struct {
	Header *tar.Header `json:"header"`
	Offset int64       `json:"offset"`
}

// TarManifest represents the metadata for the entire tar archive.
// It is typically serialized to JSON.
type TarManifest struct {
	Files []TarFileHeader `json:"files"`
}

// Pack creates a tar archive from the given source path and writes it to the writer.
// The source can be a single file or a directory.
func Pack(srcPath string, writer io.Writer) error {
	tw := tar.NewWriter(writer)
	defer tw.Close()

	// Determine the base directory to create relative paths in the tar archive.
	// This ensures that if you tar "/tmp/foo", the paths in the archive are like "foo/bar.txt", not "/tmp/foo/bar.txt".
	baseDir := filepath.Dir(srcPath)

	return filepath.Walk(srcPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Create a tar header from the file info.
		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return fmt.Errorf("creating tar header for %s: %w", path, err)
		}

		// Set the name to be relative to the source path's parent directory.
		relPath, err := filepath.Rel(baseDir, path)
		if err != nil {
			return fmt.Errorf("getting relative path for %s: %w", path, err)
		}
		header.Name = relPath

		// Write the header to the tar archive.
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("writing tar header for %s: %w", header.Name, err)
		}

		// If it's a regular file, write its content.
		if !info.Mode().IsRegular() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening file %s: %w", path, err)
		}
		defer file.Close()

		if _, err := io.Copy(tw, file); err != nil {
			return fmt.Errorf("copying file content for %s: %w", path, err)
		}

		return nil
	})
}

// Unpack extracts a tar archive from the reader to the destination path.
func Unpack(reader io.Reader, destPath string) error {
	tr := tar.NewReader(reader)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("reading next tar entry: %w", err)
		}

		// Construct the full destination path for the entry.
		target := filepath.Join(destPath, header.Name)

		// Security check: prevent path traversal attacks (e.g., "../../../etc/passwd").
		if !strings.HasPrefix(target, filepath.Clean(destPath)+string(os.PathSeparator)) {
			return fmt.Errorf("unsafe file path in tar archive: %s", header.Name)
		}

		// Handle different entry types.
		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory if it doesn't exist.
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("creating directory %s: %w", target, err)
			}
		case tar.TypeReg:
			// Create the file.
			outFile, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("creating file %s: %w", target, err)
			}

			// Copy the file content.
			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				return fmt.Errorf("writing file content for %s: %w", target, err)
			}
			outFile.Close()
		default:
			// Handle other types like symlinks if needed.
			// For now, we'll just log them.
			// log.Printf("Unsupported tar entry type %c for %s", header.Typeflag, header.Name)
		}
	}
	return nil
}

// PackToWriters splits a source path into a data stream and a metadata stream.
// It writes all file contents concatenated to dataWriter and a JSON manifest
// (containing tar headers and offsets) to headerWriter.
func PackToWriters(srcPath string, headerWriter io.Writer, dataWriter io.Writer) error {
	manifest := TarManifest{Files: []TarFileHeader{}}
	var currentOffset int64 = 0

	baseDir := filepath.Dir(srcPath)

	err := filepath.Walk(srcPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return fmt.Errorf("creating tar header for %s: %w", path, err)
		}

		relPath, err := filepath.Rel(baseDir, path)
		if err != nil {
			return fmt.Errorf("getting relative path for %s: %w", path, err)
		}
		header.Name = relPath

		fileMeta := TarFileHeader{
			Header: header,
			Offset: currentOffset,
		}
		manifest.Files = append(manifest.Files, fileMeta)

		if !info.Mode().IsRegular() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening file %s: %w", path, err)
		}
		defer file.Close()

		written, err := io.Copy(dataWriter, file)
		if err != nil {
			return fmt.Errorf("copying file content for %s: %w", path, err)
		}
		currentOffset += written

		return nil
	})

	if err != nil {
		return err
	}

	// Serialize and write the manifest to the header writer.
	jsonEncoder := json.NewEncoder(headerWriter)
	if err := jsonEncoder.Encode(manifest); err != nil {
		return fmt.Errorf("failed to encode manifest to JSON: %w", err)
	}

	return nil
}

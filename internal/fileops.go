package internal

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
)

func SerializeToFile(data interface{}, file *os.File) (err error) {
	encoder := gob.NewEncoder(file)
	if err = encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to serialize: %w", err)
	}

	return err
}

func DeserializeFromFile(file *os.File, data interface{}) (err error) {
	decoder := gob.NewDecoder(file)
	if err = decoder.Decode(data); err != nil {
		return fmt.Errorf("failed to deserialize: %w", err)
	}

	return nil
}

func WriteAll(file *os.File, buf []byte) (int, error) {
	total := 0
	remaining := len(buf)
	for remaining > 0 {
		n, err := file.Write(buf[total:])
		if err != nil {
			return total, fmt.Errorf("failed to write file: %w", err)
		}

		total += n
		remaining -= n
	}

	return total, nil
}

func WriteReadCloserToFile(rc io.ReadCloser, filePath string) (int64, error) {
	defer func() {
		if err := rc.Close(); err != nil {
			return
		}
	}()

	file, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	n, err := io.Copy(file, rc)
	if err != nil {
		return n, fmt.Errorf("failed to write file: %w", err)
	}

	return n, nil
}

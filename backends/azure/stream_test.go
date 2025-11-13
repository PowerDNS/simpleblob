package azure

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStreamReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer tearDown()

	b := getBackend(ctx, t)

	// Test reading from non-existent blob
	reader, err := b.NewReader(ctx, "does-not-exist")
	assert.Error(t, err)
	assert.Nil(t, reader)
	assert.ErrorContains(t, err, "BlobNotFound")

	// Store test data
	testData := []byte("test data for streaming")
	err = b.Store(ctx, "test-stream", testData)
	assert.NoError(t, err)

	// Test successful reading
	reader, err = b.NewReader(ctx, "test-stream")
	assert.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, testData, data)

	// Test with global prefix
	b.setGlobalPrefix("prefix/")

	// Store test data with prefix
	err = b.Store(ctx, "test-stream-prefix", testData)
	assert.NoError(t, err)

	// Read with prefix
	reader, err = b.NewReader(ctx, "test-stream-prefix")
	assert.NoError(t, err)
	defer reader.Close()

	data, err = io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestStreamWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer tearDown()

	b := getBackend(ctx, t)

	// Test streaming writer
	writer, err := b.NewWriter(ctx, "test-stream-write")
	assert.NoError(t, err)

	testData := []byte("test data for streaming writer")
	n, err := writer.Write(testData)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)

	// File should not exist before closing
	_, err = b.Load(ctx, "test-stream-write")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "BlobNotFound")

	// Close to finalize the upload
	err = writer.Close()
	assert.NoError(t, err)

	// Verify data was written correctly
	data, err := b.Load(ctx, "test-stream-write")
	assert.NoError(t, err)
	assert.Equal(t, testData, data)

	// Cannot write after close
	_, err = writer.Write([]byte("more data"))
	assert.Error(t, err)

	// Test with update marker
	b.opt.UseUpdateMarker = true

	writer, err = b.NewWriter(ctx, "test-stream-marker")
	assert.NoError(t, err)

	testData = []byte("test data with marker")
	_, err = writer.Write(testData)
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	// Verify marker is set
	assert.NotEmpty(t, b.lastMarker)

	// Verify marker file is created
	markerData, err := b.Load(ctx, UpdateMarkerFilename)
	assert.NoError(t, err)
	assert.Equal(t, b.lastMarker, string(markerData))
}

func TestStreamLargeData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer tearDown()

	b := getBackend(ctx, t)

	// Create large test data (1MB)
	size := 1024 * 1024
	largeData := make([]byte, size)
	for i := range size {
		largeData[i] = byte(i % 256)
	}

	// Test writing large data
	writer, err := b.NewWriter(ctx, "large-test")
	assert.NoError(t, err)

	n, err := writer.Write(largeData)
	assert.NoError(t, err)
	assert.Equal(t, size, n)

	err = writer.Close()
	assert.NoError(t, err)

	// Test reading large data
	reader, err := b.NewReader(ctx, "large-test")
	assert.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, largeData, readData)

	// Test writing in chunks
	writer, err = b.NewWriter(ctx, "chunk-test")
	assert.NoError(t, err)

	chunkSize := 64 * 1024
	for i := 0; i < size; i += chunkSize {
		end := i + chunkSize
		if end > size {
			end = size
		}
		_, err := writer.Write(largeData[i:end])
		assert.NoError(t, err)
	}

	err = writer.Close()
	assert.NoError(t, err)

	// Verify chunked write
	chunkData, err := b.Load(ctx, "chunk-test")
	assert.NoError(t, err)
	assert.Equal(t, largeData, chunkData)
}

func TestStreamMultipleWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer tearDown()

	b := getBackend(ctx, t)

	// Test multiple writes
	writer, err := b.NewWriter(ctx, "multi-write")
	assert.NoError(t, err)

	buffer := bytes.NewBuffer(nil)

	for i := range 10 {
		data := []byte("part " + string(rune('0'+i)))
		buffer.Write(data) // Keep track of expected data

		n, err := writer.Write(data)
		assert.NoError(t, err)
		assert.Equal(t, len(data), n)
	}

	err = writer.Close()
	assert.NoError(t, err)

	// Verify concatenated writes
	data, err := b.Load(ctx, "multi-write")
	assert.NoError(t, err)
	assert.Equal(t, buffer.Bytes(), data)
}

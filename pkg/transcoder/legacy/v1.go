package legacy

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// V1ReadMessage reads a single message from a version 1 format file
// Format: message size (8 bytes) + message data (variable)
// The timestamp has already been read by the caller and is passed in timestampBuf.
// If dataBuf is non-nil, it is grown as needed and reused; the returned slice
// is valid only until the next V1ReadMessage call. If dataBuf is nil, a new buffer is allocated.
// Returns timestamp, message data, and error. The key is always nil for version 1 format.
func V1ReadMessage(reader io.Reader, timestampBuf []byte, sizeBuf []byte, preserveTimestamps bool, dataBuf *[]byte) (time.Time, []byte, error) {
	// Read message size (8 bytes)
	if _, err := io.ReadFull(reader, sizeBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return time.Time{}, nil, io.EOF
		}
		return time.Time{}, nil, fmt.Errorf("failed to read message size: %w", err)
	}

	messageSize := int64(binary.BigEndian.Uint64(sizeBuf))
	if messageSize < 0 || messageSize > 100*1024*1024 { // Sanity check: max 100MB
		return time.Time{}, nil, fmt.Errorf("invalid message size: %d bytes", messageSize)
	}

	// Read message data: reuse/grow dataBuf when provided
	n := int(messageSize)
	var messageData []byte
	if dataBuf != nil {
		if cap(*dataBuf) < n {
			*dataBuf = make([]byte, n, max(n*2, 65536))
		}
		messageData = (*dataBuf)[:n]
	} else {
		messageData = make([]byte, n)
	}
	if _, err := io.ReadFull(reader, messageData); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return time.Time{}, nil, io.EOF
		}
		return time.Time{}, nil, fmt.Errorf("failed to read message data: %w", err)
	}

	// Parse timestamp from the already-read timestampBuf
	var msgTime time.Time
	if preserveTimestamps {
		// Read Unix timestamp (int64, big-endian)
		unixTimestamp := int64(binary.BigEndian.Uint64(timestampBuf))
		msgTime = time.Unix(unixTimestamp, 0).UTC()
	} else {
		msgTime = time.Now().UTC()
	}

	return msgTime, messageData, nil
}

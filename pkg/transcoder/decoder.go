package transcoder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrBufferTooSmall is returned when the provided key/data buffers don't have
// enough capacity for the next message. The decoder will rewind to the start
// of the message so callers can retry with larger buffers.
var ErrBufferTooSmall = errors.New("buffer too small")

// BufferTooSmallError reports the required sizes for the next message.
type BufferTooSmallError struct {
	KeyNeeded  int
	DataNeeded int
}

func (e *BufferTooSmallError) Error() string {
	return fmt.Sprintf("buffer too small (need key=%dB data=%dB)", e.KeyNeeded, e.DataNeeded)
}

func (e *BufferTooSmallError) Unwrap() error { return ErrBufferTooSmall }

// DecodeReader decodes messages from a binary file format
// Supports both version 1 (legacy, no keys) and version 2 (with keys)
type DecodeReader struct {
	reader             io.ReadSeeker
	timestampBuf       []byte
	keySizeBuf         []byte
	sizeBuf            []byte
	preserveTimestamps bool
	dataStartOffset    int64 // Offset after the header where message data starts
	protocolVersion    int32
}

// NewDecodeReader creates a new decoder for binary message files
// It reads and validates the file header, then positions the reader at the start of message data
// Supports both version 1 (legacy) and version 2 formats
func NewDecodeReader(reader io.ReadSeeker, preserveTimestamps bool) (*DecodeReader, error) {
	d := &DecodeReader{
		reader:             reader,
		timestampBuf:       make([]byte, TimestampSize),
		keySizeBuf:         make([]byte, KeySizeFieldSize),
		sizeBuf:            make([]byte, SizeFieldSize),
		preserveTimestamps: preserveTimestamps,
	}

	// Read and validate file header
	if err := d.readFileHeader(); err != nil {
		return nil, fmt.Errorf("failed to read file header: %w", err)
	}

	// Store the offset after the header for reset operations
	d.dataStartOffset = HeaderSize

	return d, nil
}

// Read reads the next complete message into the provided key and data buffers.
// This is a no-grow API: the decoder never allocates to resize the buffers.
// If the provided buffers are too small, it returns ErrBufferTooSmall (wrapped
// in *BufferTooSmallError) and rewinds the reader back to the start of the
// message so callers can retry with larger buffers.
//
// Note: key/data are passed by value; the decoder does not (and cannot) resize
// the caller's slice headers. Use the returned keyLen/dataLen to slice the
// buffers to the valid region (e.g. key[:keyLen], data[:dataLen]).
// Returns the message timestamp, the number of bytes read for key, the number
// of bytes read for data, and an error.
func (d *DecodeReader) Read(key []byte, data []byte) (time.Time, int, int, error) {
	startOffset, _ := d.reader.Seek(0, io.SeekCurrent)

	// Read timestamp (8 bytes Unix timestamp)
	if _, err := io.ReadFull(d.reader, d.timestampBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return time.Time{}, 0, 0, io.EOF
		}
		return time.Time{}, 0, 0, fmt.Errorf("failed to read timestamp: %w", err)
	}

	if d.protocolVersion == ProtocolVersion1 {
		// Version 1 format: timestamp, message size, message data (no key)
		if _, err := io.ReadFull(d.reader, d.sizeBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return time.Time{}, 0, 0, io.EOF
			}
			return time.Time{}, 0, 0, fmt.Errorf("failed to read message size: %w", err)
		}

		messageSize := int64(binary.BigEndian.Uint64(d.sizeBuf))
		if messageSize < 0 || messageSize > 100*1024*1024 { // Sanity check: max 100MB
			return time.Time{}, 0, 0, fmt.Errorf("invalid message size: %d bytes", messageSize)
		}
		dataLen := int(messageSize)

		if cap(data) < dataLen {
			_, _ = d.reader.Seek(startOffset, io.SeekStart)
			return time.Time{}, 0, dataLen, &BufferTooSmallError{KeyNeeded: 0, DataNeeded: dataLen}
		}
		db := data[:dataLen:dataLen]
		if _, err := io.ReadFull(d.reader, db); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return time.Time{}, 0, 0, io.EOF
			}
			return time.Time{}, 0, 0, fmt.Errorf("failed to read message data: %w", err)
		}

		var msgTime time.Time
		if d.preserveTimestamps {
			unixTimestamp := int64(binary.BigEndian.Uint64(d.timestampBuf))
			msgTime = time.Unix(unixTimestamp, 0).UTC()
		} else {
			msgTime = time.Now().UTC()
		}

		return msgTime, 0, dataLen, nil
	}

	// Version 2 format: timestamp, key size, message size, key, message data
	// Read key size (8 bytes)
	if _, err := io.ReadFull(d.reader, d.keySizeBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return time.Time{}, 0, 0, io.EOF
		}
		return time.Time{}, 0, 0, fmt.Errorf("failed to read key size: %w", err)
	}

	keySize := int64(binary.BigEndian.Uint64(d.keySizeBuf))
	if keySize < 0 || keySize > 100*1024*1024 { // Sanity check: max 100MB
		return time.Time{}, 0, 0, fmt.Errorf("invalid key size: %d bytes", keySize)
	}

	// Read message size (8 bytes)
	if _, err := io.ReadFull(d.reader, d.sizeBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return time.Time{}, 0, 0, io.EOF
		}
		return time.Time{}, 0, 0, fmt.Errorf("failed to read message size: %w", err)
	}

	messageSize := int64(binary.BigEndian.Uint64(d.sizeBuf))
	if messageSize < 0 || messageSize > 100*1024*1024 { // Sanity check: max 100MB
		return time.Time{}, 0, 0, fmt.Errorf("invalid message size: %d bytes", messageSize)
	}

	keyLen := int(keySize)
	dataLen := int(messageSize)

	// No-grow: require enough capacity; if not, rewind and report needed sizes.
	if (keyLen > 0 && cap(key) < keyLen) || cap(data) < dataLen {
		_, _ = d.reader.Seek(startOffset, io.SeekStart)
		return time.Time{}, keyLen, dataLen, &BufferTooSmallError{KeyNeeded: keyLen, DataNeeded: dataLen}
	}

	// Read key bytes (if present)
	if keyLen > 0 {
		kb := key[:keyLen:keyLen]
		if _, err := io.ReadFull(d.reader, kb); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return time.Time{}, 0, 0, io.EOF
			}
			return time.Time{}, 0, 0, fmt.Errorf("failed to read key data: %w", err)
		}
	}

	// Read data bytes
	db := data[:dataLen:dataLen]
	if _, err := io.ReadFull(d.reader, db); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return time.Time{}, 0, 0, io.EOF
		}
		return time.Time{}, 0, 0, fmt.Errorf("failed to read message data: %w", err)
	}

	var msgTime time.Time
	if d.preserveTimestamps {
		unixTimestamp := int64(binary.BigEndian.Uint64(d.timestampBuf))
		msgTime = time.Unix(unixTimestamp, 0).UTC()
	} else {
		msgTime = time.Now().UTC()
	}

	return msgTime, keyLen, dataLen, nil
}

// Close closes the underlying reader if it implements io.Closer
func (d *DecodeReader) Close() error {
	if closer, ok := d.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Reset seeks back to the start of message data (after the header)
func (d *DecodeReader) Reset() error {
	_, err := d.reader.Seek(d.dataStartOffset, io.SeekStart)
	return err
}

// readFileHeader reads and validates the file header
func (d *DecodeReader) readFileHeader() error {
	var headerBuf [HeaderSize]byte
	if _, err := io.ReadFull(d.reader, headerBuf[:]); err != nil {
		return err
	}

	// Read protocol version (int32, big-endian)
	d.protocolVersion = int32(binary.BigEndian.Uint32(headerBuf[0:HeaderVersionSize]))

	// Validate protocol version (support version 1 and 2)
	if d.protocolVersion != ProtocolVersion1 && d.protocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version: %d (supported versions: %d, %d)", d.protocolVersion, ProtocolVersion1, ProtocolVersion)
	}

	// Reserved bytes are read but not used yet

	return nil
}

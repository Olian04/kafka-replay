package pkg

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/lolocompany/kafka-replay/pkg/transcoder"
)

type CatConfig struct {
	Reader             io.ReadSeeker
	PreserveTimestamps bool
	Formatter          func(timestamp time.Time, data []byte) []byte
	Output             io.Writer
	FindBytes          []byte // Optional byte sequence to search for in messages
	CountOnly          bool   // If true, only count messages without outputting them
}

func Cat(ctx context.Context, cfg CatConfig) (int, error) {
	if cfg.Output == nil && !cfg.CountOnly {
		return 0, errors.New("output is required")
	}
	decoder, err := transcoder.NewDecodeReader(cfg.Reader, cfg.PreserveTimestamps)
	if err != nil {
		return 0, err
	}
	defer decoder.Close()

	count := 0

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}

		// Read next complete message
		timestamp, data, err := decoder.Read()
		if err != nil {
			if err == io.EOF {
				// End of file reached
				break
			}
			return count, err
		}

		// Filter by find bytes if specified
		if cfg.FindBytes != nil && !bytes.Contains(data, cfg.FindBytes) {
			continue
		}

		// Increment count
		count++

		// Skip formatting and writing if count-only mode
		if cfg.CountOnly {
			continue
		}

		// Display message
		formattedMessage := cfg.Formatter(timestamp, data)
		if _, err := cfg.Output.Write(formattedMessage); err != nil {
			return count, err
		}
	}
	return count, nil
}

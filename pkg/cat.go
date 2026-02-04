package pkg

import (
	"context"
	"fmt"
	"io"
)

type CatConfig struct {
	Reader       io.ReadSeeker
	TimeProvider TimeProvider
	Formatter    func(msg *RecordedMessage) string
	Output       io.Writer
}

func Cat(ctx context.Context, cfg CatConfig) error {
	msgReader := NewMessageFileReader(cfg.Reader, true, cfg.TimeProvider)
	defer msgReader.Close()

	var messageCount int64

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next complete message
		msg, err := msgReader.ReadNextMessage(ctx)
		if err != nil {
			if err == io.EOF {
				// End of file reached
				break
			}
			return err
		}

		// Display message
		formattedMessage := cfg.Formatter(msg)
		if cfg.Output != nil {
			fmt.Fprintf(cfg.Output, "%s\n", formattedMessage)
		}

		messageCount++
	}

	return nil
}

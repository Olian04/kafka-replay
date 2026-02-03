package pkg

import (
	"context"
	"fmt"
	"io"
)

func Cat(ctx context.Context, input string, formatter func(msg *LogFileMessage) string) error {
	// Create log file reader (preserve timestamps for display)
	reader, err := NewLogFileReader(input, true)
	if err != nil {
		return err
	}
	defer reader.Close()

	var messageCount int64

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next complete message
		msg, err := reader.ReadNextMessage(ctx)
		if err != nil {
			if err == io.EOF {
				// End of file reached
				break
			}
			return err
		}

		// Display message
		formattedMessage := formatter(msg)
		fmt.Printf("%s\n", formattedMessage)

		messageCount++
	}

	return nil
}

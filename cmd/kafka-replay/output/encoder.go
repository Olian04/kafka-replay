package output

import (
	"encoding/json"
	"fmt"
	"io"
)

// Encoder writes structured output in the configured format.
type Encoder struct {
	format Format
	w      io.Writer
}

// NewEncoder returns an encoder that writes to w in the given format.
func NewEncoder(format Format, w io.Writer) *Encoder {
	return &Encoder{format: format, w: w}
}

// EncodeSlice writes items as JSON (one object per line) or table format.
// For table, items must be map-like or structs that we can represent as rows;
// use EncodeTable for explicit headers/rows.
func EncodeSlice[T any](e *Encoder, items []T) error {
	switch e.format {
	case FormatJSON:
		// JSON format: one JSON object per line (JSONL-style)
		enc := json.NewEncoder(e.w)
		for _, item := range items {
			if err := enc.Encode(item); err != nil {
				return fmt.Errorf("encode json: %w", err)
			}
		}
		return nil
	case FormatTable:
		// Table format: fall back to JSON for EncodeSlice
		// (EncodeTable should be used for table output)
		enc := json.NewEncoder(e.w)
		for _, item := range items {
			if err := enc.Encode(item); err != nil {
				return fmt.Errorf("encode: %w", err)
			}
		}
		return nil
	default:
		// Raw or unknown: fall back to JSON
		enc := json.NewEncoder(e.w)
		for _, item := range items {
			if err := enc.Encode(item); err != nil {
				return fmt.Errorf("encode: %w", err)
			}
		}
		return nil
	}
}

// EncodeTable writes a table with the given headers and rows to e.w.
// Only used when format is FormatTable; otherwise no-op or fallback.
func (e *Encoder) EncodeTable(headers []string, rows [][]string) error {
	if e.format != FormatTable {
		// For non-table formats, write a simple row-based representation
		for _, row := range rows {
			for i, cell := range row {
				if i > 0 {
					fmt.Fprint(e.w, "\t")
				}
				fmt.Fprint(e.w, cell)
			}
			fmt.Fprint(e.w, "\n")
		}
		return nil
	}
	// Compute column widths
	widths := make([]int, len(headers))
	for i, h := range headers {
		if len(h) > widths[i] {
			widths[i] = len(h)
		}
	}
	for _, row := range rows {
		for i := 0; i < len(widths) && i < len(row); i++ {
			if len(row[i]) > widths[i] {
				widths[i] = len(row[i])
			}
		}
	}
	// Print header
	for i, h := range headers {
		if i > 0 {
			fmt.Fprint(e.w, "  ")
		}
		fmt.Fprintf(e.w, "%-*s", widths[i], h)
	}
	fmt.Fprint(e.w, "\n")
	for _, row := range rows {
		for i := 0; i < len(headers); i++ {
			if i > 0 {
				fmt.Fprint(e.w, "  ")
			}
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			fmt.Fprintf(e.w, "%-*s", widths[i], cell)
		}
		fmt.Fprint(e.w, "\n")
	}
	return nil
}

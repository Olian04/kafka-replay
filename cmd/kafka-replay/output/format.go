package output

import (
	"fmt"
	"strings"
)

// Format is the output format for list commands.
type Format string

const (
	FormatJSON  Format = "json"  // One JSON object per line (JSONL-style)
	FormatTable Format = "table" // Formatted table (default)
	FormatRaw   Format = "raw"   // Raw bytes (cat command only)
)

// ParseFormat parses the output format string. If s is empty, it returns
// FormatTable (default). Format "json" outputs one JSON object per line.
func ParseFormat(s string, isTTY bool) (Format, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return FormatTable, nil // Always default to table
	}
	switch Format(s) {
	case FormatJSON, FormatTable, FormatRaw:
		return Format(s), nil
	default:
		return "", fmt.Errorf("unsupported output format %q (use table, json, or raw)", s)
	}
}

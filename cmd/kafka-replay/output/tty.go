package output

import (
	"io"
	"os"

	"golang.org/x/term"
)

// IsTTY reports whether w is a terminal. Used to choose default output format
// (e.g. table when attached to a TTY, jsonl when piped).
func IsTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

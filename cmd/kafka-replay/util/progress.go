package util

import (
	"io"

	"github.com/schollz/progressbar/v3"
)

// ProgressSpinner wraps a progressbar spinner
type ProgressSpinner struct {
	bar *progressbar.ProgressBar
}

// AddBytes adds bytes to the spinner for speed/total display
func (p *ProgressSpinner) AddBytes(delta int64) {
	if p.bar != nil {
		_ = p.bar.Add(int(delta))
	}
}

// Close closes the spinner
func (p *ProgressSpinner) Close() error {
	if p.bar != nil {
		return p.bar.Close()
	}
	return nil
}

// NewProgressSpinner creates an indeterminate progress spinner
func NewProgressSpinner(description string) *ProgressSpinner {
	return &ProgressSpinner{
		bar: progressbar.DefaultBytes(-1, description),
	}
}

// CountingWriter wraps a writer to count bytes for the spinner.
// If spinner is nil, the writer is returned unchanged (no counting).
func CountingWriter(writer io.Writer, spinner *ProgressSpinner) io.WriteCloser {
	if spinner == nil {
		return &writeCloser{Writer: writer, closer: writer}
	}
	counter := &byteCounter{spinner: spinner}
	multiWriter := io.MultiWriter(writer, counter)
	return &writeCloser{
		Writer: multiWriter,
		closer: writer,
	}
}

type byteCounter struct {
	spinner *ProgressSpinner
}

func (bc *byteCounter) Write(p []byte) (int, error) {
	bc.spinner.AddBytes(int64(len(p)))
	return len(p), nil
}

type writeCloser struct {
	io.Writer
	closer io.Writer
}

func (wc *writeCloser) Close() error {
	if closer, ok := wc.closer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// CountingReadSeeker wraps a ReadSeeker to count bytes for the spinner.
// If spinner is nil, the seeker is returned unchanged (no counting).
// We need a wrapper struct because io.TeeReader only returns io.Reader, not io.ReadSeeker.
// When Seek is called, we recreate the TeeReader to read from the new position.
func CountingReadSeeker(seeker io.ReadSeeker, spinner *ProgressSpinner) io.ReadSeeker {
	if spinner == nil {
		return seeker
	}
	counter := &byteCounter{spinner: spinner}
	teeReader := io.TeeReader(seeker, counter)
	return &readSeeker{
		reader:  teeReader,
		seeker:  seeker,
		counter: counter,
	}
}

type readSeeker struct {
	reader  io.Reader
	seeker  io.ReadSeeker
	counter io.Writer
}

func (rs *readSeeker) Read(p []byte) (int, error) {
	return rs.reader.Read(p)
}

func (rs *readSeeker) Seek(offset int64, whence int) (int64, error) {
	pos, err := rs.seeker.Seek(offset, whence)
	if err != nil {
		return pos, err
	}
	rs.reader = io.TeeReader(rs.seeker, rs.counter)
	return pos, nil
}

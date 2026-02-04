package main

import (
	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/schollz/progressbar/v3"
)

// ProgressBarReporter wraps a progressbar.ProgressBar to implement pkg.ProgressReporter
type ProgressBarReporter struct {
	bar *progressbar.ProgressBar
}

func (p *ProgressBarReporter) SetTotal(total int64) {
	// For progressbar, SetTotal is handled during creation
	// This method is a no-op for progressbar as it's set at creation time
}

func (p *ProgressBarReporter) Add(delta int64) {
	if p.bar != nil {
		_ = p.bar.Add(int(delta))
	}
}

func (p *ProgressBarReporter) Set(current int64) {
	if p.bar != nil {
		_ = p.bar.Set64(current)
	}
}

func (p *ProgressBarReporter) Close() error {
	if p.bar != nil {
		return p.bar.Close()
	}
	return nil
}

// NewRecordProgressReporter creates a progress reporter for the record command
func NewRecordProgressReporter(limit int) pkg.ProgressReporter {
	var bar *progressbar.ProgressBar
	if limit > 0 {
		bar = progressbar.Default(int64(limit), "Recording messages")
	} else {
		bar = progressbar.Default(-1, "Recording messages")
	}
	return &ProgressBarReporter{bar: bar}
}

// NewReplayProgressReporter creates a progress reporter for the replay command
func NewReplayProgressReporter(fileSize int64, loop bool) pkg.ProgressReporter {
	description := "Replaying messages"
	if loop {
		description = "Replaying messages (looping)"
	}
	bar := progressbar.DefaultBytes(fileSize, description)
	return &ProgressBarReporter{bar: bar}
}

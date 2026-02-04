package pkg

// ProgressReporter reports progress updates
type ProgressReporter interface {
	SetTotal(total int64)
	Add(delta int64)
	Set(current int64)
	Close() error
}

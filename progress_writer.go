package main

import (
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"
)

// ProgressWriter wraps an io.Writer and logs progress at a fixed interval
// using a background goroutine. Call Stop() when done to clean up.
type ProgressWriter struct {
	w       io.Writer
	written atomic.Int64
	stop    chan struct{}
	done    chan struct{}
}

func NewProgressWriter(w io.Writer, interval time.Duration) *ProgressWriter {
	pw := &ProgressWriter{
		w:    w,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go pw.logLoop(interval)
	return pw
}

func (pw *ProgressWriter) logLoop(interval time.Duration) {
	defer close(pw.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf("progress: %s written", humanBytes(pw.Total()))
		case <-pw.stop:
			return
		}
	}
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if n > 0 {
		pw.written.Add(int64(n))
	}
	return n, err
}

// Stop terminates the background logging goroutine and waits for it to exit.
func (pw *ProgressWriter) Stop() {
	close(pw.stop)
	<-pw.done
}

func (pw *ProgressWriter) Total() int64 {
	return pw.written.Load()
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

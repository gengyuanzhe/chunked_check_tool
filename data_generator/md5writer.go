package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
)

type MD5Record struct {
	Bucket string
	Key    string
	MD5    string
}

// MD5Writer is a single-goroutine file writer for md5 records. Workers
// call Write (concurrent-safe, blocks when the channel is full); the
// internal goroutine serializes fmt.Fprintf onto a 64KB bufio.Writer.
//
// Close cancels the internal ctx; the writer drains any in-flight records
// before flushing and closing the file. Write after Close returns the
// writer's terminal error (or nil if it shut down cleanly).
type MD5Writer struct {
	ch     chan MD5Record
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	err    error
	mu     sync.Mutex
	file   *os.File
	bw     *bufio.Writer
}

func NewMD5Writer(path string) (*MD5Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create md5 file: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &MD5Writer{
		ch:     make(chan MD5Record, 1024),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		file:   f,
		bw:     bufio.NewWriterSize(f, 64*1024),
	}
	go w.run()
	return w, nil
}

func (w *MD5Writer) run() {
	defer func() {
		if err := w.bw.Flush(); err != nil && w.err == nil {
			w.err = err
		}
		if err := w.file.Close(); err != nil && w.err == nil {
			w.err = err
		}
		close(w.done)
	}()
	for {
		select {
		case <-w.ctx.Done():
			for {
				select {
				case rec := <-w.ch:
					if err := w.writeOne(rec); err != nil {
						w.err = err
						return
					}
				default:
					return
				}
			}
		case rec := <-w.ch:
			if err := w.writeOne(rec); err != nil {
				w.err = err
				return
			}
		}
	}
}

func (w *MD5Writer) writeOne(rec MD5Record) error {
	if _, err := fmt.Fprintf(w.bw, "%s|%s|%s\n", rec.Bucket, rec.Key, rec.MD5); err != nil {
		return err
	}
	return nil
}

func (w *MD5Writer) Write(rec MD5Record) error {
	select {
	case w.ch <- rec:
		return nil
	case <-w.done:
		return w.err
	}
}

func (w *MD5Writer) Close() error {
	w.mu.Lock()
	if w.ctx.Err() != nil {
		w.mu.Unlock()
		return w.err
	}
	w.cancel()
	w.mu.Unlock()
	<-w.done
	return w.err
}

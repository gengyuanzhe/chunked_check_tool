package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Entry struct {
	Key string
	Err string
}

type Output struct {
	dir            string
	corruptedCh    chan string
	multipartCh    chan string
	listFailedCh   chan Entry
	checkFailedCh  chan Entry
	successCh      chan string
	successEnabled bool
	wg             sync.WaitGroup
	files          []*os.File
}

func NewOutput(cfg *Config) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	o := &Output{
		dir:            cfg.OutputDir,
		corruptedCh:    make(chan string, 1024),
		multipartCh:    make(chan string, 1024),
		listFailedCh:   make(chan Entry, 1024),
		checkFailedCh:  make(chan Entry, 1024),
		successCh:      make(chan string, 1024),
		successEnabled: cfg.IsSuccessLog,
	}
	if err := o.openAndStart("corrupted_objects.txt", o.corruptedCh, false); err != nil {
		return nil, err
	}
	if err := o.openAndStart("multipart_objects.txt", o.multipartCh, false); err != nil {
		return nil, err
	}
	if err := o.openAndStart("list_failed.txt", o.listFailedCh, true); err != nil {
		return nil, err
	}
	if err := o.openAndStart("check_failed.txt", o.checkFailedCh, true); err != nil {
		return nil, err
	}
	if o.successEnabled {
		if err := o.openAndStart("success_objects.log", o.successCh, false); err != nil {
			return nil, err
		}
	}
	return o, nil
}

// openAndStart opens the file (append mode) and starts a writer goroutine.
// isEntry=true means channel is chan Entry (write "key err\n"); else chan string.
func (o *Output) openAndStart(name string, ch interface{}, isEntry bool) error {
	f, err := os.OpenFile(filepath.Join(o.dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	o.files = append(o.files, f)
	w := bufio.NewWriterSize(f, 64*1024)

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		switch c := ch.(type) {
		case chan string:
			for line := range c {
				w.WriteString(line)
				w.WriteByte('\n')
			}
		case chan Entry:
			for e := range c {
				w.WriteString(e.Key)
				w.WriteByte(' ')
				w.WriteString(e.Err)
				w.WriteByte('\n')
			}
		}
		w.Flush()
	}()
	return nil
}

func (o *Output) WriteCorrupted(key string) { o.corruptedCh <- key }
func (o *Output) WriteMultipart(etag, key string) {
	o.multipartCh <- etag + "|" + key
}
func (o *Output) WriteListFailed(prefix, errStr string)  { o.listFailedCh <- Entry{Key: prefix, Err: errStr} }
func (o *Output) WriteCheckFailed(key, errStr string)    { o.checkFailedCh <- Entry{Key: key, Err: errStr} }
func (o *Output) WriteSuccess(key string) {
	if o.successEnabled {
		o.successCh <- key
	}
}

func (o *Output) Close() error {
	close(o.corruptedCh)
	close(o.multipartCh)
	close(o.listFailedCh)
	close(o.checkFailedCh)
	if o.successEnabled {
		close(o.successCh)
	}
	o.wg.Wait()
	var firstErr error
	for _, f := range o.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

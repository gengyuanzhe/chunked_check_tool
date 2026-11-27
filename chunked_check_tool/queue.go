package main

import (
	"sync"
)

type Queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []string
	closed bool
}

func NewQueue() *Queue {
	q := &Queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Push(s string) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.items = append(q.items, s)
	q.cond.Signal()
	q.mu.Unlock()
}

// Pop returns the next queued item, blocking until one arrives or the queue
// is closed. ok is false once the queue is closed and empty.
func (q *Queue) Pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return "", false
	}
	s := q.items[0]
	q.items[0] = ""
	q.items = q.items[1:]
	return s, true
}

func (q *Queue) Close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

package main

import (
	"context"
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

func (q *Queue) Pop(ctx context.Context) (string, bool) {
	// 先尝试非阻塞
	q.mu.Lock()
	if len(q.items) > 0 {
		s := q.items[0]
		q.items[0] = ""
		q.items = q.items[1:]
		q.mu.Unlock()
		return s, true
	}
	if q.closed {
		q.mu.Unlock()
		return "", false
	}
	q.mu.Unlock()

	// ctx 已取消直接返回
	select {
	case <-ctx.Done():
		return "", false
	default:
	}

	// 阻塞等待：启动一个唤醒 goroutine，ctx 取消时 Broadcast
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.cond.Broadcast()
			q.mu.Unlock()
		case <-stop:
		}
	}()

	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
		select {
		case <-ctx.Done():
			return "", false
		default:
		}
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

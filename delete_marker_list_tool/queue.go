package main

import (
	"context"
	"sync"
)

// queue 是无界前缀队列（BFS）。slice + mutex + cond，ctx-aware Pop。
// 对齐 chunked_check_tool/queue.go 的形态，但砍掉 Drain（本工具无优雅退出）。
type queue struct {
	mu   sync.Mutex
	cond *sync.Cond
	items []string
	done  bool
}

func newQueue() *queue {
	q := &queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Push 入队一个前缀。
func (q *queue) Push(p string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, p)
	q.cond.Signal()
}

// Pop 取一个前缀。返回 (prefix, true)；ctx 取消或 SignalDone 后返回 ("", false)。
func (q *queue) Pop(ctx context.Context) (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// ctx-aware：用一个 watcher goroutine 在 ctx 取消时广播 cond。
	// 简化：每次 Pop 检查 ctx，如果空且没取消就等。为避免 ctx 取消后
	// 永久阻塞，用一个 done channel + goroutine 桥接 ctx 到 cond。
	ctxDone := ctx.Done()
	for len(q.items) == 0 && !q.done {
		// 桥接 ctx.Done() 到 cond.Signal。
		stop := make(chan struct{})
		go func() {
			select {
			case <-ctxDone:
				q.mu.Lock()
				q.cond.Broadcast()
				q.mu.Unlock()
			case <-stop:
			}
		}()
		q.cond.Wait()
		close(stop)
		select {
		case <-ctxDone:
			return "", false
		default:
		}
	}
	if q.done && len(q.items) == 0 {
		return "", false
	}
	if len(q.items) == 0 {
		return "", false
	}
	p := q.items[0]
	q.items = q.items[1:]
	return p, true
}

// SignalDone 标记队列完成（inflight 归零后调用）。唤醒所有阻塞 Pop 的 worker。
func (q *queue) SignalDone() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.done = true
	q.cond.Broadcast()
}

// Len 返回当前队列长度（进度展示用）。
func (q *queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestQueuePushPop(t *testing.T) {
	q := NewQueue()
	q.Push("a")
	q.Push("b")
	got, ok := q.Pop(context.Background())
	if !ok || got != "a" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
	got, ok = q.Pop(context.Background())
	if !ok || got != "b" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
}

func TestQueueBlockingPop(t *testing.T) {
	q := NewQueue()
	done := make(chan struct{})
	go func() {
		v, ok := q.Pop(context.Background())
		if !ok || v != "x" {
			t.Errorf("got %q ok=%v", v, ok)
		}
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	q.Push("x")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Pop did not unblock after Push")
	}
}

func TestQueueCloseEmpty(t *testing.T) {
	q := NewQueue()
	q.Close()
	_, ok := q.Pop(context.Background())
	if ok {
		t.Fatal("expected ok=false after close on empty queue")
	}
}

func TestQueueCloseAfterDrain(t *testing.T) {
	q := NewQueue()
	q.Push("a")
	q.Close()
	v, ok := q.Pop(context.Background())
	if !ok || v != "a" {
		t.Fatalf("got %q ok=%v", v, ok)
	}
	_, ok = q.Pop(context.Background())
	if ok {
		t.Fatal("expected ok=false after drain")
	}
}

func TestQueueConcurrent(t *testing.T) {
	q := NewQueue()
	const N = 1000
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < N; j++ {
				q.Push("x")
			}
		}()
	}
	go func() {
		wg.Wait()
		q.Close()
	}()
	count := 0
	for {
		_, ok := q.Pop(context.Background())
		if !ok {
			break
		}
		count++
	}
	if count != 5*N {
		t.Fatalf("count=%d want %d", count, 5*N)
	}
}

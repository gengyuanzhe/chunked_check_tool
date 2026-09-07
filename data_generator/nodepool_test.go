package main

import "testing"

func TestNodePool_AssignRoundRobin(t *testing.T) {
	p := &NodePool{
		endpoints: []string{"1.1.1.1:80", "2.2.2.2:80", "3.3.3.3:80"},
		scheme:    "http",
	}
	want := []int{0, 1, 2, 0, 1, 2, 0}
	for i, w := range want {
		got := p.Assign(i)
		if got != w {
			t.Errorf("Assign(%d) = %d, want %d", i, got, w)
		}
	}
}

func TestNodePool_AssignNegative(t *testing.T) {
	p := &NodePool{
		endpoints: []string{"a:80", "b:80"},
		scheme:    "http",
	}
	got := p.Assign(-5)
	if got != 1 {
		t.Errorf("Assign(-5) = %d, want 1 (modulo wraparound)", got)
	}
}

func TestNodePool_Endpoint(t *testing.T) {
	p := &NodePool{
		endpoints: []string{"1.1.1.1:80", "2.2.2.2:80"},
		scheme:    "https",
	}
	if got := p.Endpoint(0); got != "1.1.1.1:80" {
		t.Errorf("Endpoint(0) = %q, want 1.1.1.1:80", got)
	}
	if got := p.Endpoint(1); got != "2.2.2.2:80" {
		t.Errorf("Endpoint(1) = %q, want 2.2.2.2:80", got)
	}
}

func TestNodePool_URL(t *testing.T) {
	p := &NodePool{
		endpoints: []string{"1.1.1.1:80"},
		scheme:    "https",
	}
	if got := p.URL(0); got != "https://1.1.1.1:80" {
		t.Errorf("URL(0) = %q, want https://1.1.1.1:80", got)
	}
}

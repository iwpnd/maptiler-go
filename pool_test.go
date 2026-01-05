package maptiler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWorkerPoolProcessTasks(t *testing.T) {
	s := &testProcessor{}

	wp := newPool(s, withPoolConcurrency(2), withPoolQueueSize(10))

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	numTestTasks := 2
	go func() {
		for i := range numTestTasks {
			task := newTask(fmt.Sprintf("palimpalim-%d", i))
			wp.Enqueue(task)
		}
		wp.Stop()
	}()

	err := wp.Start(ctx)
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if got := s.Count(); got != numTestTasks {
		t.Fatalf("expected %d processed tasks, got %d", numTestTasks, got)
	}
}

type errorProcessor struct {
	tasks sync.Map
}

func (e *errorProcessor) Process(ctx context.Context, t task[string]) error {
	// when we receive "fail", we force an error
	if t.Body == "fail" {
		return fmt.Errorf("processing failed")
	}
	e.tasks.Store(t.ID.String(), t)
	return nil
}

func (e *errorProcessor) Close() {}

func TestWorkerPoolTaskError(t *testing.T) {
	s := &errorProcessor{}

	wp := newPool(s, withPoolConcurrency(2), withPoolQueueSize(10))

	ctx := t.Context()

	go func() {
		wp.Enqueue(newTask("ok"))
		wp.Enqueue(newTask("fail"))
		wp.Stop()
	}()

	err := wp.Start(ctx)
	if err == nil {
		t.Fatalf("expected error from Start, got nil")
	}
	if !strings.Contains(err.Error(), "processing failed") {
		t.Fatalf("expected error to contain %q, got %v", "processing failed", err)
	}
}

func TestWorkerContextCancelled(t *testing.T) {
	ctx := t.Context()
	ctx, cancel := context.WithCancel(ctx)

	p := &testProcessor{}

	wp := newPool(p, withPoolConcurrency(2), withPoolQueueSize(10))

	// cancel before starting
	cancel()

	err := wp.Start(ctx)
	if err == nil {
		t.Fatalf("expected error due to context cancellation, got nil")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected error to contain %q, got %v", "context canceled", err)
	}
}

type testProcessor struct {
	tasks sync.Map
}

func (d *testProcessor) Process(ctx context.Context, t task[string]) error {
	d.tasks.Store(t.ID.String(), t)
	return nil
}

func (d *testProcessor) Close() {}

func (d *testProcessor) Count() int {
	var i int
	d.tasks.Range(func(k, v any) bool {
		i++
		return true
	})
	return i
}

package maptiler

import (
	"context"

	"golang.org/x/sync/errgroup"
)

const (
	defaultQueueSize   = 100
	defaultConcurrency = 10
)

// poolConfig holds configuration values for the worker pool.
type poolConfig struct {
	queueSize   int
	concurrency int
}

type poolOption func(*poolConfig)

// withPoolConcurrency allows controlling the number of parallel workers.
func withPoolConcurrency(c int) poolOption {
	return func(config *poolConfig) {
		config.concurrency = c
	}
}

// withPoolQueueSize allows controlling the task channel buffer size.
func withPoolQueueSize(qs int) poolOption {
	return func(config *poolConfig) {
		config.queueSize = qs
	}
}

// pool is a generic worker pool that delegates processing tasks to a Processor.
type pool[T any] struct {
	processor processor[T]
	config    *poolConfig
	tasks     chan task[T]
}

// newPool creates a new worker pool for tasks of type T.
func newPool[T any](
	processor processor[T],
	options ...poolOption,
) *pool[T] {
	config := &poolConfig{
		concurrency: defaultConcurrency,
		queueSize:   defaultQueueSize,
	}
	for _, o := range options {
		o(config)
	}

	return &pool[T]{
		tasks:     make(chan task[T], config.queueSize),
		processor: processor,
		config:    config,
	}
}

// Start launches the worker goroutines.
func (wp *pool[T]) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for range wp.config.concurrency {
		g.Go(func() error {
			return wp.process(ctx)
		})
	}
	return g.Wait()
}

// Stop closes the tasks channel.
func (wp *pool[T]) Stop() {
	close(wp.tasks)
}

// Enqueue adds a task to the tasks channel.
func (wp *pool[T]) Enqueue(t task[T]) {
	wp.tasks <- t
}

// process reads tasks from the channel and processes them using the given Processor.
func (wp *pool[T]) process(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t, ok := <-wp.tasks:
			if !ok {
				return nil
			}
			if err := wp.processor.Process(ctx, t); err != nil {
				return err
			}
		}
	}
}

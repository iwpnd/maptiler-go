package maptiler

import "github.com/segmentio/ksuid"

// task is a generic type that holds any payload.
type task[T any] struct {
	Body T
	ID   ksuid.KSUID
}

// newTask creates a new Task with the provided payload.
func newTask[T any](body T) task[T] {
	return task[T]{
		ID:   ksuid.New(),
		Body: body,
	}
}

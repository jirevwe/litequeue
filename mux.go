package litequeue

import (
	"context"
	"fmt"
	"sync"
)

type Mux struct {
	entries map[string]muxEntry
	mu      *sync.RWMutex
}

type muxEntry struct {
	h    Handler
	name string
}

func NewMux() *Mux {
	return &Mux{
		entries: make(map[string]muxEntry),
		mu:      &sync.RWMutex{},
	}
}

// Handle is used to register a handler func given a task name
func (m *Mux) Handle(name string, h Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries[name] = muxEntry{
		h:    h,
		name: name,
	}
}

// match finds a handler in entries given a task name.
func (m *Mux) match(taskname string) (h Handler) {
	// only check for exact match for now.
	v, ok := m.entries[taskname]
	if ok {
		return v.h
	}

	// todo: implement partial matches

	return nil
}

// ProcessTask dispatches the task to the handler whose
// pattern most closely matches the task type.
func (m *Mux) ProcessTask(ctx context.Context, task *Task) error {
	h := m.Handler(task)
	return h.ProcessTask(ctx, task)
}

// Handler returns the handler to use for the given task.
// It always returns a non-nil handler.
//
// If there is no registered handler that applies to the task,
// handler returns a 'not found' handler which returns an error.
func (m *Mux) Handler(t *Task) (h Handler) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	h = m.match(t.Type())
	if h == nil {
		h = NotFoundHandler()
	}

	return h
}

// NotFound returns an error indicating that the handler was not found for the given task.
func NotFound(ctx context.Context, task *Task) error {
	return fmt.Errorf("handler not found for task %q", task.Type())
}

// NotFoundHandler returns a simple task handler that returns a “not found“ error.
func NotFoundHandler() Handler { return HandlerFunc(NotFound) }

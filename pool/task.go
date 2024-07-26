package pool

import (
	"log/slog"
	"sync"
)

type Task struct {
	typeName string
	message  []byte
	log      *slog.Logger
	Options  TaskOption

	// leave these here as placeholders for now
	Execute func(task *Task) error
	OnError func(error)
}

func (t *Task) Type() string    { return t.typeName }
func (t *Task) Payload() []byte { return t.message }

func NewTask(message []byte, log *slog.Logger) *Task {
	return &Task{
		message: message,
		log:     log,
	}
}

type TaskOption struct {
	// todo: add task proper task options
	options map[string]any
	mu      *sync.Mutex
}

func (o *TaskOption) AddOption(key string, value any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.options[key] = value
}

func (o *TaskOption) GetOption(key string) *any {
	o.mu.Lock()
	defer o.mu.Unlock()
	if value, ok := o.options[key]; ok {
		return &value
	}
	return nil
}

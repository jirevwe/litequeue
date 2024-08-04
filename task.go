package litequeue

import (
	"log/slog"
	"sync"
)

type Task struct {
	taskid   string
	typeName string
	message  []byte
	log      *slog.Logger
	Options  TaskOption
}

func (t *Task) Id() string      { return t.taskid }
func (t *Task) Type() string    { return t.typeName }
func (t *Task) Payload() []byte { return t.message }

func NewTask(message []byte, queueId string, log *slog.Logger) *Task {
	return &Task{
		typeName: queueId,
		message:  message,
		log:      log,
	}
}

func (t *Task) WithTaskId(id string) *Task {
	t.taskid = id
	return t
}

type TaskOption struct {
	// todo: Register task proper task options
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

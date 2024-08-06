package litequeue

import (
	"sync"
)

type Task struct {
	taskId   string
	typeName string
	message  []byte
	Options  TaskOption
}

func (t *Task) Id() string      { return t.taskId }
func (t *Task) Type() string    { return t.typeName }
func (t *Task) Payload() []byte { return t.message }

func NewTask(message []byte, queueId string) *Task {
	return &Task{
		typeName: queueId,
		message:  message,
	}
}

func (t *Task) WithTaskId(id string) *Task {
	t.taskId = id
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

type TaskInfo struct {
	task        *Task
	statusLevel TaskStatusLevel
}

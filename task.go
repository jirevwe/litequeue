package litequeue

import "time"

type Task struct {
	taskId   string
	typeName string
	message  []byte
	delay    time.Duration
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

func (t *Task) WithDelay(delay time.Duration) *Task {
	t.delay = delay
	return t
}

func (t *Task) Delay() time.Duration {
	return t.delay
}

type TaskInfo struct {
	task        *Task
	statusLevel TaskStatusLevel
}

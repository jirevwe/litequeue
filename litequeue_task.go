package litequeue

import (
	"github.com/jirevwe/litequeue/queue"
	"log"
)

type Task struct {
	executeFunc func() error
	job         *queue.LiteMessage
}

func NewLiteQueueTask(job *queue.LiteMessage) *Task {
	return &Task{
		job: job,
	}
}

func (l Task) Execute() error {
	if l.executeFunc != nil {
		return l.executeFunc()
	}

	return nil
}

func (l Task) OnFailure(err error) {
	log.Printf("%+v\n", err)
}

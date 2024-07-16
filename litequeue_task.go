package litequeue

import (
	"fmt"
	"github.com/jirevwe/litequeue/packer"
	"github.com/jirevwe/litequeue/queue"
	"log"
)

type Task struct {
	job *queue.LiteMessage
}

func NewLiteQueueTask(job *queue.LiteMessage) *Task {
	return &Task{
		job: job,
	}
}

func (l *Task) Execute() error {
	fmt.Printf("%+v\n", fmt.Sprintf("task: id:%s, msg:%s\n", l.job.Id, l.job.Message))
	return nil
}

func (l *Task) OnFailure(err error) {
	log.Printf("%+v\n", err)
}

func (l *Task) Marshal() ([]byte, error) {
	return packer.EncodeMessage(l.job)
}

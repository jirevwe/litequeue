package litequeue

import (
	"context"
	"fmt"
	"github.com/jirevwe/litequeue/pool"
	"github.com/jirevwe/litequeue/queue"
	"log/slog"
	"time"
)

// LiteQueue is the shell for the application
type LiteQueue struct {
	workerPool pool.Pool
	queue      queue.Queue
	ctx        context.Context
	logger     *slog.Logger
}

func (q *LiteQueue) Start() {
	q.workerPool.Start()

	// we need to poll the db for new jobs
	for {
		// todo: poll from all queues it is hardcoded atm
		liteMessage, err := q.queue.Consume(q.ctx, "local_queue")
		if err != nil {
			q.logger.Error(err.Error(), "func", "queue.Consume")
		}

		if &liteMessage == nil {
			// nothing to work on, sleep then try again
			time.Sleep(time.Second)
			continue
		}

		q.logger.Info(fmt.Sprintf("liteMessage: %+v", liteMessage))

		job := NewLiteQueueTask(&liteMessage)
		err = q.workerPool.AddWork(job)
		if err != nil {
			q.logger.Error(err.Error(), "func", "workerPool.AddWork")
		}

		time.Sleep(time.Second)
	}
}

// todo: write to accept a lite-task interface
func (q *LiteQueue) Write(ctx context.Context, queueName string, task *Task) error {
	raw, err := task.Marshal()
	if err != nil {
		return err
	}

	return q.queue.Write(ctx, queueName, raw)
}

package litequeue

import (
	"context"
	"fmt"
	"github.com/jirevwe/litequeue/pool"
	"github.com/jirevwe/litequeue/queue"
	"github.com/jirevwe/litequeue/queue/sqlite"
	"github.com/oklog/ulid/v2"
	"log/slog"
	"time"
)

// LiteQueue is the shell for the application
type LiteQueue struct {
	workerPool pool.Pool
	queue      queue.Queue
	ctx        context.Context
	logger     *slog.Logger
	notifyChan chan pool.Task
}

func NewLiteQueue(ctx context.Context, dbPath string, logger *slog.Logger) (*LiteQueue, error) {
	s, err := sqlite.NewSqlite(dbPath, logger)
	if err != nil {
		return nil, err
	}

	notifyChan := make(chan pool.Task, 1000)
	wp := pool.NewWorkerPool(10, 10, logger, notifyChan)

	q := &LiteQueue{
		queue:      s,
		ctx:        ctx,
		logger:     logger,
		notifyChan: notifyChan,
		workerPool: wp,
	}

	return q, nil
}

func (q *LiteQueue) CreateQueue(ctx context.Context, queueName string) error {
	return q.queue.CreateQueue(ctx, queueName)
}

func (q *LiteQueue) Start() {
	q.workerPool.Start()

	// we need to poll the db for new jobs
	for {
		select {
		case <-q.ctx.Done():
			break
		case task := <-q.notifyChan:
			q.logger.Info(fmt.Sprintf("%v", task))
		default:
		}

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

		//q.logger.Info(fmt.Sprintf("liteMessage: %+v", liteMessage))

		job := NewLiteQueueTask([]byte(liteMessage.Message), q.logger)
		err = q.workerPool.AddWork(job)
		if err != nil {
			q.logger.Error(err.Error(), "func", "workerPool.AddWork")
		}

		time.Sleep(time.Second)
	}
}

// todo: write to accept a lite-task interface
func (q *LiteQueue) Write(ctx context.Context, queueName string, task *Task) error {
	job := &queue.LiteMessage{
		Id:        ulid.Make().String(),
		Message:   string(task.Message),
		VisibleAt: time.Now().Add(30 * time.Second).String(),
	}

	raw, err := job.Marshal()
	if err != nil {
		return err
	}

	return q.queue.Write(ctx, queueName, raw)
}

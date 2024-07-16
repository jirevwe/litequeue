package litequeue

import (
	"context"
	"github.com/jirevwe/litequeue/pool"
	"github.com/jirevwe/litequeue/queue"
	"github.com/jirevwe/litequeue/queue/sqlite"
	"github.com/oklog/ulid/v2"
	"log"
	"time"
)

func Main() {
	testQueueName := "local_queue"

	sqliteQueue, err := sqlite.NewSqlite(testQueueName)
	if err != nil {
		log.Fatalln(err)
	}

	lite := LiteQueue{
		workerPool: pool.NewWorkerPool(10, 10),
		ctx:        context.Background(),
		queue:      sqliteQueue,
		queueName:  testQueueName,
	}

	t := NewLiteQueueTask(&queue.LiteMessage{
		Id:        ulid.Make().String(),
		Message:   "hello world!",
		VisibleAt: time.Now().Add(30 * time.Second).String(),
	})

	err = lite.Write(context.Background(), testQueueName, t)
	if err != nil {
		log.Fatalln(err)
	}

	lite.Start()
}

func (q *LiteQueue) Start() {
	q.workerPool.Start()

	// we need to poll the db for new jobs
	for {
		// todo: poll from all queues
		liteMessage, err := q.queue.Consume(q.ctx, q.queueName)
		if err != nil {
			// todo: configure library structured logger
			log.Printf("Consume :%+v\n", err)
		}

		if &liteMessage == nil {
			// nothing to work on, sleep then try again
			time.Sleep(time.Second)
			continue
		}

		log.Printf("liteMessage: %+v\n", liteMessage)

		job := NewLiteQueueTask(&liteMessage)
		err = q.workerPool.AddWork(job)
		if err != nil {
			log.Printf("%+v\n", err)
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

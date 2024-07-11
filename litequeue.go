package litequeue

import (
	"context"
	"github.com/jirevwe/litequeue/pool"
	"github.com/jirevwe/litequeue/queue"
	"github.com/jirevwe/litequeue/queue/sqlite"
	"log"
)

func Main() {
	testQueueName := "test_queue"

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

	lite.Start()
}

// LiteQueue is the shell for the application
type LiteQueue struct {
	workerPool pool.Pool
	queue      queue.Queue
	ctx        context.Context
	queueName  string
}

func (q *LiteQueue) Start() {
	go q.workerPool.Start()

	// we need to pool the db for new jobs
	for {
		// todo: poll from all queues
		job, err := q.queue.Consume(q.ctx, q.queueName)
		if err != nil {
			log.Printf("%+v\n", err)
		}

		if job != nil {
			continue
		}

		log.Printf("%+v\n", job)
		err = q.workerPool.AddWork(NewLiteQueueTask(job))
		if err != nil {
			log.Printf("%+v\n", err)
		}
	}
}

func (q *LiteQueue) Write(ctx context.Context, payload []byte) error {
	// todo: the "queue" should be passed to LiteQueue.Write()
	return q.queue.Write(ctx, q.queueName, payload)
}

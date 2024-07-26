package litequeue

import (
	"context"
	"fmt"
	"github.com/jirevwe/litequeue/pool"
	"github.com/jirevwe/litequeue/queue"
	"github.com/jirevwe/litequeue/queue/sqlite"
	"github.com/jirevwe/litequeue/util"
	"github.com/oklog/ulid/v2"
	"log/slog"
	"sync"
	"time"
)

// LiteQueue is the shell for the application
type LiteQueue struct {
	workerPool pool.Pool
	queue      queue.Queue
	ctx        context.Context
	logger     *slog.Logger
	notifyChan chan *pool.Task
	mux        Mux
}

type HandlerFunc func(*pool.Task) error

type Mux struct {
	queueList map[string]HandlerFunc
	mu        *sync.Mutex
}

func (m *Mux) add(name string, h HandlerFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueList[name] = h
}

func NewLiteQueue(ctx context.Context, dbPath string, logger *slog.Logger) (*LiteQueue, error) {
	s, err := sqlite.NewSqlite(dbPath, logger)
	if err != nil {
		return nil, err
	}

	notifyChan := make(chan *pool.Task, 10)
	wp := pool.NewWorkerPool(10, 10, logger, notifyChan)
	mux := Mux{queueList: make(map[string]HandlerFunc), mu: &sync.Mutex{}}

	q := &LiteQueue{
		queue:      s,
		ctx:        ctx,
		logger:     logger,
		notifyChan: notifyChan,
		workerPool: wp,
		mux:        mux,
	}

	return q, nil
}

func (q *LiteQueue) CreateQueue(ctx context.Context, queueName string, executeFunc HandlerFunc) error {
	// try to create the queue, if it succeeds, add it to the list of queues
	if err := q.queue.CreateQueue(ctx, queueName); err != nil {
		return err
	}

	// todo: figure our how to properly register handlers for queues when creating them
	q.mux.add(queueName, executeFunc)

	return nil
}

func (q *LiteQueue) Start() {
	// todo: poll from all queues it is hardcoded atm
	queueName := "local_queue"

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

		liteMessage, err := q.queue.Consume(q.ctx, queueName)
		if err != nil {
			q.logger.Error(err.Error(), "func", "queue.Consume")
		}

		if &liteMessage == nil {
			// nothing to work on, sleep then try again
			time.Sleep(time.Second)
			continue
		}

		task := pool.NewTask([]byte(liteMessage.Message), q.logger)
		task.Execute = q.mux.queueList[queueName]
		task.OnError = func(err error) {
			q.logger.Error(err.Error(), "func", "queue.OnError")
		}

		err = q.workerPool.AddWork(task)
		if err != nil {
			q.logger.Error(err.Error(), "func", "workerPool.AddWork")
		}

		time.Sleep(time.Second)
	}
}

func (q *LiteQueue) Write(ctx context.Context, queueName string, task *pool.Task) error {
	job := &queue.LiteMessage{
		Id:        ulid.Make().String(),
		Message:   string(task.Payload()),
		VisibleAt: util.NewRealClock().Now().Add(30 * time.Second).String(),
	}

	raw, err := job.Marshal()
	if err != nil {
		return err
	}

	return q.queue.Write(ctx, queueName, raw)
}

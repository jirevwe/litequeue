package litequeue

import (
	"context"
	"github.com/oklog/ulid/v2"
	"log/slog"
	"os"
	"time"
)

type Server struct {
	ctx        context.Context
	mux        *Mux
	queue      Queue
	logger     *slog.Logger
	workerPool Pool

	// todo: change this to TaskInfo
	// started is a channel used to signal up a layer about work starting status
	started chan *Task

	// todo: change this to TaskInfo
	// finished is a channel used to signal up a layer about work completion status
	finished chan *Task
}

type Config struct {
	mux    *Mux
	queue  Queue
	logger *slog.Logger
	dbPath string
}

func NewServer(cfg *Config) (*Server, error) {
	if cfg.logger == nil {
		cfg.logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}

	if cfg.queue == nil {
		s, err := NewSqlite(cfg.dbPath, cfg.logger)
		if err != nil {
			return nil, err
		}
		cfg.queue = s
	}

	if cfg.mux == nil {
		cfg.mux = NewMux()
	}

	started := make(chan *Task, 10)
	finished := make(chan *Task, 10)
	workerPool := NewWorkerPool(10, 10, cfg.logger, started, finished, cfg.mux)

	q := &Server{
		ctx:        context.Background(),
		workerPool: workerPool,
		logger:     cfg.logger,
		queue:      cfg.queue,
		finished:   finished,
		mux:        cfg.mux,
		started:    started,
	}

	return q, nil
}

func (q *Server) CreateQueue(ctx context.Context, queueName string, handlerFunc HandlerFunc) error {
	// fetch the queue, so we don't have to create it again if it already exists
	if !q.queue.QueueExists(ctx, queueName) {
		// try to create the queue, if it succeeds, Register it to the list of queues
		if err := q.queue.CreateQueue(ctx, queueName); err != nil {
			return err
		}
	}

	// todo: figure our how to properly register handlers for queues when creating them
	q.mux.Handle(queueName, handlerFunc)

	return nil
}

func (q *Server) Start() {
	q.workerPool.Start()

	// we need to poll the db for new jobs
	for {
		select {
		case <-q.ctx.Done():
			break
		case task := <-q.started:
			_, err := q.queue.UpdateMessageStatus(q.ctx, task.Id(), Active)
			if err != nil {
				q.logger.Error(err.Error(), "source", "started")
			}
		case task := <-q.finished:
			_, err := q.queue.UpdateMessageStatus(q.ctx, task.Id(), Completed)
			if err != nil {
				q.logger.Error(err.Error(), "source", "finished")
			}
		default:
		}

		liteMessage, err := q.queue.Pop(q.ctx)
		if err != nil {
			q.logger.Error(err.Error(), "func", "queue.Pop")
		}

		if &liteMessage == nil || len(liteMessage.Id) == 0 {
			// nothing to work on, sleep then try again
			time.Sleep(time.Second)
			continue
		}

		task := NewTask([]byte(liteMessage.Message), liteMessage.QueueId, q.logger).WithTaskId(liteMessage.Id)
		err = q.workerPool.AddWork(task)
		if err != nil {
			q.logger.Error(err.Error(), "func", "workerPool.AddWork")
		}

		time.Sleep(time.Second)
	}
}

func (q *Server) Write(ctx context.Context, queueName string, task *Task) error {
	job := &LiteMessage{
		Id:        ulid.Make().String(),
		Message:   string(task.Payload()),
		VisibleAt: NewRealClock().Now().Add(30 * time.Second).String(),
	}

	raw, err := job.Marshal()
	if err != nil {
		return err
	}

	return q.queue.Push(ctx, queueName, raw)
}

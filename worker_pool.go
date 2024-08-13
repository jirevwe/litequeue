package litequeue

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

var ErrWorkerPoolClosed = errors.New("worker pool is not active")

type WorkerPool struct {
	// channel from which workers consume work
	tasks chan *Task

	// ensure the pool can only be started once
	start sync.Once

	// ensure the pool can only be stopped once
	stop sync.Once

	// channel to signal all the workers to stop
	globalQuit chan bool

	workers []*Worker

	wg *sync.WaitGroup

	mux *Mux

	log *slog.Logger

	started chan *TaskInfo

	finished chan *TaskInfo

	// todo: implement prometheus to expose metrics about 1) work done (failed and successful tasks) 2) how many goroutines are being created 3) latency of a queue 4) task throughput
}

func (p *WorkerPool) Start() {
	p.start.Do(func() {
		p.log.Info("starting worker pool")
		p.startWorkers()
	})
}

func (p *WorkerPool) startWorkers() {
	for i := 0; i < len(p.workers); i++ {
		w := NewWorker(fmt.Sprintf("worker_%d", i+1), p.tasks, p.globalQuit, p.started, p.finished, p.wg, p.log, p.mux)
		p.workers[i] = w
		p.wg.Add(1)
		go w.Start()
	}
}

// AddWorkNonBlocking adds work to the WorkerPool and returns immediately
func (p *WorkerPool) AddWorkNonBlocking(t *Task, errChan chan error) {
	go func() {
		err := p.AddWork(t)
		if err != nil {
			if errChan != nil {
				errChan <- err
			} else {
				p.log.Error(err.Error())
			}
		}
	}()
}

func (p *WorkerPool) Stop() error {
	p.stop.Do(func() {
		// We need to close the "tasks" channel, and need to decide two things:
		// a) should all the queued tasks be processed before returning?
		// b) should Stop() block until all tasks are done and the workers return?

		p.log.Info("stopping worker pool")

		// tell each worker to stop processing tasks
		close(p.globalQuit)

		// close the channel on which we receive new jobs
		close(p.tasks)

		// wait for all of them to clean themselves up
		p.wg.Wait()

		p.log.Info("worker pool has been stopped\n")
	})
	return nil
}

// AddWork adds work to the WorkerPool. If the channel buffer is full (or 0) and
// all workers are occupied, this will block until work is consumed or Stop() is called.
func (p *WorkerPool) AddWork(t *Task) error {
	select {
	case <-p.globalQuit:
		return ErrWorkerPoolClosed
	default:
		p.tasks <- t
	}

	return nil
}

func NewWorkerPool(numWorkers uint, log *slog.Logger, started chan *TaskInfo, finished chan *TaskInfo, mux *Mux) Pool {
	// size of the internal queue
	tasks := make(chan *Task, 1000)

	return &WorkerPool{
		// number of workers in the pool
		workers:    make([]*Worker, numWorkers),
		globalQuit: make(chan bool, 1),
		wg:         &sync.WaitGroup{},
		stop:       sync.Once{},
		start:      sync.Once{},
		finished:   finished,
		started:    started,
		tasks:      tasks,
		mux:        mux,
		log:        log,
	}
}

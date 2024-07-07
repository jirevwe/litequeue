package pool

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

var ErrWorkerPoolClosed = errors.New("worker pool is not active")

type Pool interface {
	// Start gets the worker pool ready-to-process jobs, and should only be called once
	Start()

	// Stop stops the worker pool, tears down any required resources,
	// and should only be called once
	Stop() error

	// AddWork adds a task for the worker pool to process. It is only valid after
	// Start() has been called and before Stop() has been called.
	AddWork(Task) error
}

type WorkerPool struct {
	// channel from which workers consume work
	tasks chan Task

	// ensure the pool can only be started once
	start sync.Once

	// ensure the pool can only be stopped once
	stop sync.Once

	// channel to signal all the workers to stop
	globalQuit chan bool

	stopInProgress bool

	workers []*Worker

	mutex sync.Mutex

	wg *sync.WaitGroup
}

func (p *WorkerPool) Start() {
	p.start.Do(func() {
		log.Println("starting worker pool")
		p.startWorkers()
	})
}

func (p *WorkerPool) startWorkers() {
	for i := 0; i < len(p.workers); i++ {
		w := NewWorker(fmt.Sprintf("worker_%d", i+1), p.tasks, p.globalQuit, p.wg)
		p.workers[i] = w
		go w.Start()
		p.wg.Add(1)
	}
}

// AddWorkNonBlocking adds work to the WorkerPool and returns immediately
func (p *WorkerPool) AddWorkNonBlocking(t Task) {
	go func() {
		err := p.AddWork(t)
		if err != nil {
			log.Println(err)
		}
	}()
}

func (p *WorkerPool) Stop() error {
	p.stop.Do(func() {
		// We need to close the "tasks" channel, and need to decide two things:
		// a) should all the queued tasks be processed before returning?
		// b) should Stop() block until all tasks are done and the workers return?

		p.mutex.Lock()
		p.stopInProgress = true
		p.mutex.Unlock()

		log.Printf("stopping worker pool")

		// tell each worker to stop processing tasks
		close(p.globalQuit)

		// wait for all of them to clean themselves up
		p.wg.Wait()

		// close the channel on which we receive new jobs
		close(p.tasks)

		log.Printf("worker pool has been stopped\n")
	})
	return nil
}

// AddWork adds work to the WorkerPool. If the channel buffer is full (or 0) and
// all workers are occupied, this will block until work is consumed or Stop() is called.
func (p *WorkerPool) AddWork(t Task) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	println("WorkerPool.AddWork")

	if p.stopInProgress {
		return ErrWorkerPoolClosed
	}

	p.tasks <- t

	return nil
}

func NewWorkerPool(numWorkers, size uint) Pool {
	// size of the internal queue
	tasks := make(chan Task, size)

	return &WorkerPool{
		// number of workers in the pool
		workers:        make([]*Worker, numWorkers),
		tasks:          tasks,
		start:          sync.Once{},
		stop:           sync.Once{},
		globalQuit:     make(chan bool, 1),
		stopInProgress: false,
		mutex:          sync.Mutex{},
		wg:             &sync.WaitGroup{},
	}
}

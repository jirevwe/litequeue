package litequeue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Worker is a worker instance
type Worker struct {
	// the worker id
	id string

	// channel from which the worker consumes work
	tasks chan *Task

	// channel to signal the worker to stop working
	quit chan bool

	// used to signal the pool to clean itself u
	wg *sync.WaitGroup

	mux      *Mux
	started  chan *Task
	finished chan *Task
	log      *slog.Logger
}

func NewWorker(id string, tasks chan *Task, quit chan bool, started chan *Task, finished chan *Task, wg *sync.WaitGroup, log *slog.Logger, mux *Mux) *Worker {
	return &Worker{
		id:       id,
		wg:       wg,
		mux:      mux,
		log:      log,
		quit:     quit,
		tasks:    tasks,
		started:  started,
		finished: finished,
	}
}

func (w *Worker) Start() {
	w.log.Info(fmt.Sprintf("starting worker %s", w.id))

	defer func() {
		w.wg.Done()
		w.log.Info(fmt.Sprintf("worker %s has been stopped", w.id))
	}()

	for {

		// if there are tasks, process one
		if len(w.tasks) > 0 {
			task, ok := <-w.tasks
			if !ok {
				w.log.Info(fmt.Sprintf("stopping worker %s with closed tasks channel", w.id))
				return
			}

			// todo: should we check if the task's context is done?

			// todo: fix the race condition where the status could be set to completed then set to active after

			// notify that the task is "Active"
			w.started <- task

			// find the task's exec func and run it
			err := w.mux.ProcessTask(context.Background(), task)
			if err != nil {
				w.log.Error(fmt.Sprintf("worker %s failed to execute task: %s", w.id, err.Error()))
			}

			// todo: we write to channel to notify the pool that work was done or failed
			// notify that the task is "Completed" or "Failed"
			w.finished <- task

			continue
		}

		// check for quit
		select {
		case <-w.quit:
			w.log.Info(fmt.Sprintf("stopping worker %s with quit channel", w.id))

			// when quit is called, how can I ensure that the currently executing task is completed
			// before returning?
			return
		default:
			// do nothing
		}
	}
}

package pool

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

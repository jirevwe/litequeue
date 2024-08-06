package litequeue

type Pool interface {
	// Start gets the worker pool ready-to-process jobs, and should only be called once
	Start()

	// Stop stops the worker pool, tears down any required resources,
	// and should only be called once
	Stop() error

	// AddWork adds a task for the worker pool to process. It is only valid after
	// Start() has been called and before Stop() has been called.
	AddWork(*Task) error

	// AddWorkNonBlocking adds a task for the worker pool to process but doesn't return an error
	// It is only valid after, it passes the error to a channel.
	// Start() has been called and before Stop() has been called.
	AddWorkNonBlocking(*Task, chan error)
}

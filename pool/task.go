package pool

type Task interface {
	// Execute performs the work
	Execute() error

	// OnFailure handles any error returned from Execute()
	OnFailure(error)
}

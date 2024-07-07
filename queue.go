package litequeue

import "context"

//{SCHEMA}.{TABLE_PREFIX}__{TABLE_NAME}  -- will hold non-terminal messages for the queue
//{SCHEMA}.{TABLE_PREFIX}__{TABLE_NAME}_archived -- will hold archived messages for the queue
//
//ltq.queues__PriorityQueue
//ltq.queues__PriorityQueue_archived

type queue interface {
	// Write puts an item on a queue
	Write(context.Context, string, []byte) error

	// Consume fetches the first visible item from a queue
	Consume(context.Context, string) ([]byte, error)

	// Delete removes a message from a queue
	Delete(context.Context, string) error
}

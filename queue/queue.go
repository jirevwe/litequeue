package queue

import (
	"context"
	"github.com/jirevwe/litequeue/packer"
)

//{SCHEMA}.{TABLE_PREFIX}__{TABLE_NAME}  -- will hold non-terminal messages for the Queue
//{SCHEMA}.{TABLE_PREFIX}__{TABLE_NAME}_archived -- will hold archived messages for the Queue
//
//ltq.queues__PriorityQueue
//ltq.queues__PriorityQueue_archived

type Queue interface {
	// Write puts an item on a Queue
	Write(context.Context, string, []byte) error

	// Consume fetches the first visible item from a Queue
	Consume(context.Context, string) (LiteMessage, error)

	// Delete removes a message from a Queue
	Delete(context.Context, string, string) error

	CreateQueue(context.Context, string) error

	DeleteQueue(context.Context, string) error
}

type LiteMessage struct {
	Id        string `json:"id" db:"id"`
	Status    string `json:"status" db:"status"`
	Message   string `json:"message" db:"message"`
	VisibleAt string `json:"visible_at" db:"visible_at"`
	CreatedAt string `json:"created_at" db:"created_at"`
	UpdatedAt string `json:"updated_at" db:"updated_at"`
}

func (l *LiteMessage) Marshal() ([]byte, error) {
	return packer.EncodeMessage(l)
}

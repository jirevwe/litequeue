package queue

import (
	"context"
	"github.com/jirevwe/litequeue/packer"
	"time"
)

const (
	// Rfc3339Milli is like time.RFC3339Nano, but with millisecond precision
	Rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"
)

type Queue interface {
	// Write puts an item on a Queue
	Write(context.Context, string, []byte) error

	// Consume fetches the first visible item from a Queue
	Consume(context.Context, string) (LiteMessage, error)

	// Delete removes a message from a Queue
	DeleteMessage(context.Context, string, string) error

	CreateQueue(context.Context, string) error

	DeleteQueue(context.Context, string) error
}

type LiteMessage struct {
	Id        string `json:"id" db:"id"`
	Status    string `json:"status" db:"status"`
	Message   string `json:"message" db:"message"`
	QueueId   string `json:"queue_id" db:"queue_id"`
	VisibleAt string `json:"visible_at" db:"visible_at"`
	CreatedAt string `json:"created_at" db:"created_at"`
	UpdatedAt string `json:"updated_at" db:"updated_at"`
}

func (l *LiteMessage) VisibleTime() time.Time {
	t, err := time.Parse(Rfc3339Milli, l.VisibleAt)
	if err != nil {
		return time.Time{}
	}
	return t
}

func (l *LiteMessage) Marshal() ([]byte, error) {
	return packer.EncodeMessage(l)
}

func (l *LiteMessage) FormatForInserter() LiteMessageInserter {
	return LiteMessageInserter{
		Id:        l.Id,
		Status:    l.Status,
		Message:   []byte(l.Message),
		QueueId:   l.QueueId,
		VisibleAt: l.VisibleTime().Format(Rfc3339Milli),
		CreatedAt: l.VisibleTime().Format(Rfc3339Milli),
		UpdatedAt: l.VisibleTime().Format(Rfc3339Milli),
	}
}

type LiteMessageInserter struct {
	Id        string `json:"id" db:"id"`
	Status    string `json:"status" db:"status"`
	Message   []byte `json:"message" db:"message"`
	QueueId   string `json:"queue_id" db:"queue_id"`
	VisibleAt string `json:"visible_at" db:"visible_at"`
	CreatedAt string `json:"created_at" db:"created_at"`
	UpdatedAt string `json:"updated_at" db:"updated_at"`
}

type LiteQueue struct {
	Id        string `db:"id"`
	Name      string `db:"name"`
	CreatedAt string `db:"created_at"`
}

type ArchivedLiteQueue struct {
	Id         string `db:"id"`
	Name       string `db:"name"`
	CreatedAt  string `db:"created_at"`
	ArchivedAt string `db:"archived_at"`
}

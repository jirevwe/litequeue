package litequeue

import (
	"context"
	"time"
)

type (
	TaskStatus      string
	TaskStatusLevel int
)

const (
	// Rfc3339Milli is like time.RFC3339Nano, but with millisecond precision
	Rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"
)

const (
	StatusScheduled TaskStatus = "scheduled"
	StatusCompleted TaskStatus = "completed"
	StatusArchived  TaskStatus = "archived"
	StatusPending   TaskStatus = "pending"
	StatusActive    TaskStatus = "active"
	StatusRetry     TaskStatus = "retry"
)

const (
	ScheduledLevel TaskStatusLevel = iota
	PendingLevel
	ActiveLevel
	RetryLevel
	CompletedLevel
	ArchivedLevel
)

func taskStatusFromInt(i int) TaskStatusLevel {
	return TaskStatusLevel(i)
}

func taskStatusLevelFromString(s string) TaskStatusLevel {
	return taskLevelFromStatus(taskStatusFromString(s))
}

func taskStatusFromString(s string) TaskStatus {
	switch s {
	case "scheduled":
		return StatusScheduled
	case "completed":
		return StatusCompleted
	case "archived":
		return StatusArchived
	case "pending":
		return StatusPending
	case "active":
		return StatusActive
	case "retry":
		return StatusRetry
	}
	return StatusScheduled
}

func taskStatusFromLevel(l TaskStatusLevel) TaskStatus {
	switch l {
	case ScheduledLevel:
		return StatusScheduled
	case PendingLevel:
		return StatusPending
	case ActiveLevel:
		return StatusActive
	case RetryLevel:
		return StatusRetry
	case CompletedLevel:
		return StatusCompleted
	case ArchivedLevel:
		return StatusArchived
	}
	return StatusScheduled
}

func taskLevelFromStatus(l TaskStatus) TaskStatusLevel {
	switch l {
	case StatusScheduled:
		return ScheduledLevel
	case StatusPending:
		return PendingLevel
	case StatusActive:
		return ActiveLevel
	case StatusRetry:
		return RetryLevel
	case StatusCompleted:
		return CompletedLevel
	case StatusArchived:
		return ArchivedLevel
	}
	return ScheduledLevel
}

type Queue interface {
	// Push puts an item on a Queue
	Push(context.Context, *LiteMessage) error

	// Pop fetches the first visible item from a Queue
	Pop(context.Context) (LiteMessage, error)

	// DeleteMessage removes a message from a Queue
	DeleteMessage(context.Context, string, string) error

	// CreateQueue creates a queue
	CreateQueue(context.Context, string) error

	// DeleteQueue archives a queue and it's messages
	DeleteQueue(context.Context, string) error

	// QueueExists checks if the queue exists
	QueueExists(context.Context, string) bool

	// UpdateMessageStatus updates an item's status
	UpdateMessageStatus(context.Context, string, TaskStatusLevel) (LiteMessage, error)
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
		return time.Now()
	}
	return t
}

func (l *LiteMessage) Marshal() ([]byte, error) {
	return EncodeMessage(l)
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

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jirevwe/litequeue/queue"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/oklog/ulid/v2"
	"log/slog"
	"time"
)

const (
	// rfc3339Milli is like time.RFC3339Nano, but with millisecond precision
	rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"
)

type Sqlite struct {
	logger *slog.Logger
	db     *sqlx.DB
}

func NewSqlite(dbPath string, logger *slog.Logger) (*Sqlite, error) {
	db, err := sqlx.Open("sqlite3", fmt.Sprintf("%s?_journal_mode=WAL&_foreign_keys=off&_auto_vacuum=full", dbPath))
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return &Sqlite{db: db, logger: logger}, nil
}

func (s *Sqlite) CreateQueue(ctx context.Context, queueName string) error {
	return s.inTx(ctx, func(tx *sqlx.Tx) error {
		createQueueQuery := `CREATE TABLE IF NOT EXISTS queues__` + queueName + ` (
			id TEXT PRIMARY KEY,
			message BLOB,
			visible_at TEXT not null,
			status text not null default 'scheduled',
			created_at TEXT not null default (strftime('%Y-%m-%dT%H:%M:%fZ')),
			updated_at TEXT not null default (strftime('%Y-%m-%dT%H:%M:%fZ'))
		) strict;`

		createArchivedQueueQuery := `CREATE TABLE IF NOT EXISTS queues__` + queueName + `_archived (
			id TEXT PRIMARY KEY,
			message BLOB,
			status text not null,
			created_at TEXT not null,
			updated_at TEXT not null,
			archived_at TEXT not null default (strftime('%Y-%m-%dT%H:%M:%fZ'))
		) strict;`

		_, err := tx.ExecContext(ctx, createQueueQuery)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, createArchivedQueueQuery)
		if err != nil {
			return err
		}

		return nil
	})
}

func (s *Sqlite) DeleteQueue(ctx context.Context, queueName string) error {
	return s.inTx(ctx, func(tx *sqlx.Tx) error {
		deleteQueueQuery := `DELETE TABLE queues__` + queueName
		_, err := tx.ExecContext(ctx, deleteQueueQuery)
		if err != nil {
			return err
		}

		deleteArchivedQueueQuery := `DELETE TABLE queues__` + queueName + `_archived`
		_, err = tx.ExecContext(ctx, deleteArchivedQueueQuery)
		if err != nil {
			return err
		}

		return nil
	})
}

// Write puts an item on a queue
func (s *Sqlite) Write(ctx context.Context, queueName string, message []byte) error {
	// todo: expose delay as a configurable value
	now := time.Now().Add(time.Second)
	nowFormatted := now.Format(rfc3339Milli)
	name := fmt.Sprintf(" queues__%s", queueName)

	return s.inTx(ctx, func(tx *sqlx.Tx) error {
		// write to the queues
		writeQuery := `insert into ` + name + ` (id, message, visible_at) values ($1, $2, $3)`
		_, innerErr := tx.ExecContext(ctx, writeQuery, ulid.Make().String(), message, nowFormatted)
		if innerErr != nil {
			return innerErr
		}
		return nil
	})
}

type id struct {
	Id string `db:"id"`
}

// Consume fetches the first visible item from a queue
func (s *Sqlite) Consume(ctx context.Context, queueName string) (message queue.LiteMessage, err error) {
	name := fmt.Sprintf("queues__%s", queueName)
	getFirstItem := `select id from ` + name + ` where datetime(visible_at) < CURRENT_TIMESTAMP and status = 'scheduled' order by id limit 1;`
	updateItemStatus := `update ` + name + ` set status = 'pending' where id = $1 returning *;`

	defer func() {
		if errors.Is(err, sql.ErrNoRows) {
			// we don't care about "sql: no rows in result set" errors
			err = nil
		}
	}()

	err = s.inTx(ctx, func(tx *sqlx.Tx) error {
		// read one message from the queue
		row := tx.QueryRowxContext(ctx, getFirstItem)
		if row.Err() != nil {
			return row.Err()
		}

		var rowValue id
		if rowScanErr := row.StructScan(&rowValue); rowScanErr != nil {
			return rowScanErr
		}

		row = tx.QueryRowxContext(ctx, updateItemStatus, rowValue.Id)
		if row.Err() != nil {
			return row.Err()
		}

		if rowScanErr := row.StructScan(&message); rowScanErr != nil {
			return rowScanErr
		}

		return nil
	})

	return message, err
}

// Delete removes a message from a queue
func (s *Sqlite) Delete(ctx context.Context, queueName string, msgId string) (err error) {
	name := fmt.Sprintf("queues__%s", queueName)
	archivedName := fmt.Sprintf("queues__%s_archived", queueName)

	err = s.inTx(ctx, func(tx *sqlx.Tx) error {
		deleteQuery := `delete from ` + name + ` where id = $1 returning *`
		row := tx.QueryRowxContext(ctx, deleteQuery, msgId)
		if row.Err() != nil {
			return row.Err()
		}

		var msg queue.LiteMessage
		if rowScanErr := row.StructScan(&msg); rowScanErr != nil {
			return rowScanErr
		}

		writeQuery := `insert into ` + archivedName + ` (id, message, status, created_at, updated_at) values ($1, $2, $3, $4, $5)`
		_, innerErr := tx.ExecContext(ctx, writeQuery, msg.Id, []byte(msg.Message), msg.Status, msg.CreatedAt, msg.UpdatedAt)
		if innerErr != nil {
			return innerErr
		}
		return nil
	})

	return err
}

func (s *Sqlite) Truncate(ctx context.Context, queueName string) (err error) {
	_, err = s.db.ExecContext(ctx, fmt.Sprintf(`delete from queues__%s where id > '0'`, queueName))
	if err != nil {
		return err
	}
	return nil
}

func (s *Sqlite) inTx(ctx context.Context, cb func(*sqlx.Tx) error) (err error) {
	tx, txErr := s.db.BeginTxx(ctx, nil)
	if txErr != nil {
		return fmt.Errorf("cannot start tx: %w", txErr)
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = rollback(tx, nil)
			panic(rec)
		}
	}()

	if err := cb(tx); err != nil {
		return rollback(tx, err)
	}

	if txErr := tx.Commit(); txErr != nil {
		return fmt.Errorf("cannot commit tx: %w", txErr)
	}

	return nil
}

func rollback(tx *sqlx.Tx, err error) error {
	if txErr := tx.Rollback(); txErr != nil {
		return fmt.Errorf("cannot roll back tx after error (tx error: %v), original error: %w", txErr, err)
	}
	return err
}

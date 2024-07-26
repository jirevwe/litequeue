package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jirevwe/litequeue/queue"
	"github.com/jirevwe/litequeue/util"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/oklog/ulid/v2"
	"log/slog"
	"time"
)

var (
	createQueues = `create table if not exists queues (
    		id TEXT not null primary key,
    		name TEXT not null unique,
    		created_at TEXT not null default (strftime('%Y-%m-%dT%H:%M:%fZ'))
		) strict;`

	createArchivedQueues = `create table if not exists archived_queues (
    		id TEXT not null primary key,
    		name TEXT not null unique,
    		created_at TEXT not null,
    		archived_at TEXT not null default (strftime('%Y-%m-%dT%H:%M:%fZ'))
		) strict;`

	createMessages = `CREATE TABLE IF NOT EXISTS messages (
			id TEXT NOT NULL PRIMARY KEY,
			message BLOB,
			status TEXT not null default 'scheduled',
			queue_id TEXT NOT NULL,
			visible_at TEXT not null,
			created_at TEXT not null default (strftime('%Y-%m-%dT%H:%M:%fZ')),
			updated_at TEXT not null default (strftime('%Y-%m-%dT%H:%M:%fZ')),
			FOREIGN KEY(queue_id) REFERENCES queues(name)
		) strict;`

	createArchivedMessages = `CREATE TABLE IF NOT EXISTS archived_messages (
    		id TEXT NOT NULL PRIMARY KEY,
			message BLOB,
			status TEXT not null,
			queue_id TEXT NOT NULL,
			created_at TEXT not null,
			updated_at TEXT not null,
			archived_at TEXT not null default (strftime('%Y-%m-%dT%H:%M:%fZ'))
		) strict;`

	createQueue     = `INSERT INTO queues (id, name) values ($1, $2);`
	archiveMessages = `insert into archived_messages (id, message, status, queue_id, created_at, updated_at) VALUES (:id, :message, :status, :queue_id, :created_at, :updated_at)`
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

	s := &Sqlite{db: db, logger: logger}

	ctx := context.Background()
	err = s.inTx(ctx, func(tx *sqlx.Tx) error {
		// create queue table
		_, err = tx.ExecContext(ctx, createQueues)
		if err != nil {
			return err
		}

		// create archived queue table
		_, err = tx.ExecContext(ctx, createArchivedQueues)
		if err != nil {
			return err
		}

		// create message table
		_, err = tx.ExecContext(ctx, createMessages)
		if err != nil {
			return err
		}

		// create archived message table
		_, err = tx.ExecContext(ctx, createArchivedMessages)
		if err != nil {
			return err
		}

		return nil
	})

	return s, err
}

func (s *Sqlite) CreateQueue(ctx context.Context, queueName string) (err error) {
	return s.inTx(ctx, func(tx *sqlx.Tx) error {
		_, err = tx.ExecContext(ctx, createQueue, ulid.Make().String(), queueName)
		if err != nil {
			return err
		}

		return nil
	})
}

// DeleteQueue achieves the queue and it's messages, use TruncateQueue if you want to hard delete messages
func (s *Sqlite) DeleteQueue(ctx context.Context, queueName string) (err error) {
	// todo: copy the messages to the queue's archived table, delete all the messages in the table, then archive the table
	return s.inTx(ctx, func(tx *sqlx.Tx) error {
		// delete from messages
		rows, rowsErr := tx.QueryxContext(ctx, `delete from messages where queue_id = $1 returning *`, queueName)
		if rowsErr != nil {
			return rowsErr
		}
		defer rows.Close()

		var messages []queue.LiteMessageInserter
		for rows.Next() {
			msg := queue.LiteMessage{}
			if err = rows.StructScan(&msg); err != nil {
				return err
			}
			messages = append(messages, msg.FormatForInserter())
		}

		//nothing to archive, exit early
		if len(messages) == 0 {
			return nil
		}

		// insert into archived messages
		_, err = tx.NamedExecContext(ctx, archiveMessages, messages)
		if err != nil {
			return err
		}

		row := tx.QueryRowxContext(ctx, `DELETE FROM queues where name = $1 returning *`, queueName)
		if row.Err() != nil {
			if errors.Is(row.Err(), sql.ErrNoRows) {
				// can't find the queue
				return nil
			}

			return row.Err()
		}

		var q queue.LiteQueue
		if err = row.StructScan(&q); err != nil {
			return err
		}

		deleteArchivedQueueQuery := `INSERT INTO archived_queues (id, name, created_at) values ($1, $2, $3);`
		_, err = tx.ExecContext(ctx, deleteArchivedQueueQuery, q.Id, q.Name, q.CreatedAt)
		if err != nil {
			return err
		}

		return nil
	})
}

// Write puts an item on a queue
func (s *Sqlite) Write(ctx context.Context, queueId string, message []byte) error {
	// todo: expose delay as a configurable value
	now := util.NewRealClock().Now().Add(time.Second)
	nowFormatted := now.Format(queue.Rfc3339Milli)

	return s.inTx(ctx, func(tx *sqlx.Tx) error {
		// write to the queues
		writeQuery := `insert into messages (id, message, queue_id, visible_at) values ($1, $2, $3, $4)`
		_, innerErr := tx.ExecContext(ctx, writeQuery, ulid.Make().String(), message, queueId, nowFormatted)
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
	getFirstItem := `select id from messages where queue_id = $1 and datetime(visible_at) < CURRENT_TIMESTAMP and status = 'scheduled' order by id limit 1;`
	updateItemStatus := `update messages set status = 'pending' where id = $1 and queue_id = $2 returning *;`

	defer func() {
		if errors.Is(err, sql.ErrNoRows) {
			// we don't care about "sql: no rows in result set" errors
			err = nil
		}
	}()

	err = s.inTx(ctx, func(tx *sqlx.Tx) error {
		// read one message from the queue
		row := tx.QueryRowxContext(ctx, getFirstItem, queueName)
		if row.Err() != nil {
			return row.Err()
		}

		var rowValue id
		if rowScanErr := row.StructScan(&rowValue); rowScanErr != nil {
			return rowScanErr
		}

		row = tx.QueryRowxContext(ctx, updateItemStatus, rowValue.Id, queueName)
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

// DeleteMessage removes a message from a queue
func (s *Sqlite) DeleteMessage(ctx context.Context, queueName string, msgId string) (err error) {
	err = s.inTx(ctx, func(tx *sqlx.Tx) error {
		deleteQuery := `delete from messages where id = $1 and queue_id = $2 returning *`
		row := tx.QueryRowxContext(ctx, deleteQuery, msgId, queueName)
		if row.Err() != nil {
			return row.Err()
		}

		var msg queue.LiteMessage
		if rowScanErr := row.StructScan(&msg); rowScanErr != nil {
			return rowScanErr
		}

		writeQuery := `insert into archived_messages (id, message, status, queue_id, created_at, updated_at) values ($1, $2, $3, $4, $5, $6)`
		_, innerErr := tx.ExecContext(ctx, writeQuery, msg.Id, []byte(msg.Message), msg.Status, msg.QueueId, msg.CreatedAt, msg.UpdatedAt)
		if innerErr != nil {
			return innerErr
		}
		return nil
	})

	return err
}

// TruncateQueue clears the contents of a queue, use DeleteQueue if you want to archive messages
func (s *Sqlite) TruncateQueue(ctx context.Context, queueName string) (err error) {
	return s.inTx(ctx, func(tx *sqlx.Tx) error {
		_, err = tx.ExecContext(ctx, `DELETE FROM queues where name = $1`, queueName)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `delete from messages where queue_id = $1`, queueName)
		if err != nil {
			return err
		}
		return nil
	})
}

// GetArchivedMessages gets the messages on the archived queue
func (s *Sqlite) GetArchivedMessages(ctx context.Context, queueName string) (message []queue.LiteMessage, err error) {
	getArchivedMessages := `select id from archived_messages where queue_id = $1 order by id desc;`

	err = s.inTx(ctx, func(tx *sqlx.Tx) error {
		// read one message from the queue
		rows, rowsErr := tx.QueryxContext(ctx, getArchivedMessages, queueName)
		if rowsErr != nil {
			return rowsErr
		}
		defer rows.Close()

		for rows.Next() {
			var rowValue queue.LiteMessage
			if rowScanErr := rows.StructScan(&rowValue); rowScanErr != nil {
				return rowScanErr
			}
			message = append(message, rowValue)
		}

		return nil
	})

	return message, err
}

// GetArchivedQueue gets the archived queue
func (s *Sqlite) GetArchivedQueue(ctx context.Context, queueName string) (queue queue.ArchivedLiteQueue, err error) {
	getArchivedMessages := `select * from archived_queues where name = $1;`

	err = s.inTx(ctx, func(tx *sqlx.Tx) error {
		// read one message from the queue
		row := tx.QueryRowxContext(ctx, getArchivedMessages, queueName)
		if row.Err() != nil {
			return row.Err()
		}

		if rowScanErr := row.StructScan(&queue); rowScanErr != nil {
			return rowScanErr
		}

		return nil
	})

	return queue, err
}

func (s *Sqlite) inTx(ctx context.Context, cb func(*sqlx.Tx) error) (err error) {
	tx, beginErr := s.db.BeginTxx(ctx, nil)
	if beginErr != nil {
		return fmt.Errorf("cannot start tx: %w", beginErr)
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = rollback(tx, nil)
			panic(rec)
		}
	}()

	if err = cb(tx); err != nil {
		return rollback(tx, err)
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("cannot commit tx: %w", commitErr)
	}

	return nil
}

func rollback(tx *sqlx.Tx, err error) error {
	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		return fmt.Errorf("cannot roll back tx after error (tx error: %v), original error: %w", rollbackErr, err)
	}
	return err
}

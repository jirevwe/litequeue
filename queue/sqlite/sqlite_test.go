package sqlite

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var slogger = slog.New(slog.NewTextHandler(os.Stdout, nil))

func setupTest(t *testing.T) (context.Context, string, *Sqlite) {
	ctx := context.Background()
	queueName := fmt.Sprintf("test-%v", time.Now().UnixNano())

	dir, err := os.Getwd()
	if err != nil {
		slogger.Error(err.Error())
	}

	dbPath := filepath.Join(dir, "../../litequeue.db")

	s, err := NewSqlite(dbPath, slogger)
	require.NoError(t, err)

	return ctx, queueName, s
}

func teardownTest(t *testing.T, ctx context.Context, queueName string, s *Sqlite) {
	require.NoError(t, s.TruncateQueue(ctx, queueName))
}

func TestSqlite_DeleteQueue_EmptyQueue(t *testing.T) {
	ctx, queueName, s := setupTest(t)
	defer teardownTest(t, ctx, queueName, s)

	err := s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	require.NoError(t, s.DeleteQueue(ctx, queueName))

	msgs, err := s.GetArchivedMessages(ctx, queueName)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

func TestSqlite_DeleteQueue_NonEmptyQueue(t *testing.T) {
	ctx, queueName, s := setupTest(t)
	defer teardownTest(t, ctx, queueName, s)

	err := s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	err = s.Write(ctx, queueName, []byte("hello world"))
	require.NoError(t, err)

	require.NoError(t, s.DeleteQueue(ctx, queueName))

	msgs, err := s.GetArchivedMessages(ctx, queueName)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	// fetch the archived queue
	q, err := s.GetArchivedQueue(ctx, queueName)
	require.NoError(t, err)
	require.Equal(t, q.Name, queueName)
}

func TestSqlite_WriteOne(t *testing.T) {
	ctx, queueName, s := setupTest(t)
	defer teardownTest(t, ctx, queueName, s)

	err := s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	err = s.Write(ctx, queueName, []byte("hello world"))
	require.NoError(t, err)
}

func TestSqlite_WriteConcurrently(t *testing.T) {
	ctx, queueName, s := setupTest(t)
	defer teardownTest(t, ctx, queueName, s)

	wg := &sync.WaitGroup{}

	err := s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	message := []byte("hello world")

	for i := 0; i < 10; i++ {
		go writeOne(t, ctx, s, queueName, message, wg)
		wg.Add(1)
	}
	wg.Wait()
}

func TestSqlite_Consume(t *testing.T) {
	ctx, queueName, s := setupTest(t)
	defer teardownTest(t, ctx, queueName, s)

	err := s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	message := []byte("hello world")

	for i := 0; i < 2; i++ {
		writeOne(t, ctx, s, queueName, []byte(fmt.Sprintf("%s_%d", message, i)), nil)
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < 2; i++ {
		msg, consumeErr := s.Consume(ctx, queueName)
		require.NoError(t, consumeErr)
		require.Equal(t, fmt.Sprintf("%s_%d", message, i), msg.Message)
	}
}

func TestSqlite_TruncateQueue(t *testing.T) {
	ctx, queueName, s := setupTest(t)
	defer teardownTest(t, ctx, queueName, s)

	err := s.CreateQueue(ctx, queueName)
	require.NoError(t, err)

	message := []byte("hello world")

	for i := 0; i < 2; i++ {
		writeOne(t, ctx, s, queueName, []byte(fmt.Sprintf("%s_%d", message, i)), nil)
	}

	time.Sleep(2 * time.Second)

	msgIds := make([]string, 2)
	for i := 0; i < 2; i++ {
		msg, consumeErr := s.Consume(ctx, queueName)
		require.NoError(t, consumeErr)
		require.Equal(t, fmt.Sprintf("%s_%d", message, i), msg.Message)
		msgIds[i] = msg.Id
	}

	for i := 0; i < 2; i++ {
		deleteErr := s.Delete(ctx, queueName, msgIds[i])
		require.NoError(t, deleteErr)
	}
}

func writeOne(t *testing.T, ctx context.Context, ss *Sqlite, qName string, message []byte, w *sync.WaitGroup) {
	err := ss.Write(ctx, qName, message)
	require.NoError(t, err)

	if w != nil {
		w.Done()
	}
}

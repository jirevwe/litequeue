package sqlite

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestSqlite_WriteOne(t *testing.T) {
	queueName := "test_queue"
	s, err := NewSqlite(queueName)
	require.NoError(t, err)

	err = s.Write(context.Background(), queueName, []byte("hello world"))
	require.NoError(t, err)
}

func TestSqlite_WriteConcurrently(t *testing.T) {
	queueName := "test_queue"
	s, err := NewSqlite(queueName)
	require.NoError(t, err)
	wg := &sync.WaitGroup{}
	ctx := context.Background()
	message := []byte("hello world")

	for i := 0; i < 10; i++ {
		go writeOne(t, ctx, s, queueName, message, wg)
		wg.Add(1)
	}
	wg.Wait()

	require.NoError(t, s.Truncate(ctx, queueName))
}

func TestSqlite_Consume(t *testing.T) {
	queueName := "test_queue"
	s, err := NewSqlite(queueName)
	require.NoError(t, err)
	ctx := context.Background()
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

	require.NoError(t, s.Truncate(ctx, queueName))
}

func writeOne(t *testing.T, ctx context.Context, ss *Sqlite, qName string, message []byte, w *sync.WaitGroup) {
	err := ss.Write(ctx, qName, message)
	require.NoError(t, err)

	if w != nil {
		w.Done()
	}
}

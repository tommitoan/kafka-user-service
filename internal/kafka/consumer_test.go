package kafka

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tommitoan/kafka-user-service/internal/models"
)

// fakeReader replays a scripted sequence of fetch results, then blocks until ctx is done.
type fakeReader struct {
	mu        sync.Mutex
	script    []fetchResult
	committed []int64
	commitErr error
	drained   chan struct{}
	once      sync.Once
}

type fetchResult struct {
	msg kafkago.Message
	err error
}

func newFakeReader(script ...fetchResult) *fakeReader {
	return &fakeReader{script: script, drained: make(chan struct{})}
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if len(r.script) > 0 {
		next := r.script[0]
		r.script = r.script[1:]
		r.mu.Unlock()
		return next.msg, next.err
	}
	r.mu.Unlock()
	r.once.Do(func() { close(r.drained) })
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.commitErr != nil {
		return r.commitErr
	}
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

func (r *fakeReader) Committed() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.committed...)
}

func msgAt(offset int64, value string) fetchResult {
	return fetchResult{msg: kafkago.Message{Topic: "t", Partition: 0, Offset: offset, Value: []byte(value)}}
}

// runLoop drives a topicConsumer until the script is drained, then stops it.
func runLoop(t *testing.T, r *fakeReader, decode decodeFunc, h EventHandler) {
	t.Helper()
	tc := &topicConsumer{format: "test", groupID: "g", reader: r, decode: decode, backoff: time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tc.run(ctx, h) }()

	select {
	case <-r.drained:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not drain the scripted messages")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func okDecode(data []byte) (*models.UserEvent, error) {
	if string(data) == "bad" {
		return nil, errors.New("undecodable")
	}
	return &models.UserEvent{EventID: string(data)}, nil
}

func TestConsumer_CommitsOnlyAfterHandlerSucceeds(t *testing.T) {
	r := newFakeReader(msgAt(0, "a"), msgAt(1, "b"), msgAt(2, "c"))

	runLoop(t, r, okDecode, func(_ context.Context, _ MessageMeta, e *models.UserEvent) error {
		if e.EventID == "b" {
			return errors.New("boom")
		}
		return nil
	})

	assert.Equal(t, []int64{0, 2}, r.Committed(), "failed message must not be committed")
}

func TestConsumer_DoesNotCommitUndecodableMessage(t *testing.T) {
	r := newFakeReader(msgAt(0, "bad"), msgAt(1, "ok"))

	var handled []string
	runLoop(t, r, okDecode, func(_ context.Context, _ MessageMeta, e *models.UserEvent) error {
		handled = append(handled, e.EventID)
		return nil
	})

	assert.Equal(t, []string{"ok"}, handled)
	assert.Equal(t, []int64{1}, r.Committed())
}

func TestConsumer_PassesMetaToHandler(t *testing.T) {
	r := newFakeReader(fetchResult{msg: kafkago.Message{Topic: "topic-x", Partition: 3, Offset: 41, Value: []byte("a")}})

	var got MessageMeta
	runLoop(t, r, okDecode, func(_ context.Context, m MessageMeta, _ *models.UserEvent) error {
		got = m
		return nil
	})

	assert.Equal(t, MessageMeta{Topic: "topic-x", Partition: 3, Offset: 41, ConsumerGroup: "g"}, got)
}

func TestConsumer_ContinuesAfterFetchErrors(t *testing.T) {
	r := newFakeReader(
		fetchResult{err: io.EOF},
		fetchResult{err: errors.New("broker unavailable")},
		msgAt(5, "a"),
	)

	runLoop(t, r, okDecode, func(context.Context, MessageMeta, *models.UserEvent) error { return nil })

	assert.Equal(t, []int64{5}, r.Committed())
}

func TestConsumer_CommitFailureDoesNotStopLoop(t *testing.T) {
	r := newFakeReader(msgAt(0, "a"), msgAt(1, "b"))
	r.commitErr = errors.New("commit failed")

	var count int
	runLoop(t, r, okDecode, func(context.Context, MessageMeta, *models.UserEvent) error {
		count++
		return nil
	})

	assert.Equal(t, 2, count)
	assert.Empty(t, r.Committed())
}

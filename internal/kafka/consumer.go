package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/riferrei/srclient"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/tommitoan/kafka-user-service/internal/models"
)

// MessageMeta carries Kafka message metadata into the handler.
// The idempotent handler uses ConsumerGroup + Topic + event_id as the dedup key.
// Partition and Offset are stored in processed_events for observability only.
type MessageMeta struct {
	Topic         string
	Partition     int
	Offset        int64
	ConsumerGroup string
}

// EventHandler is called for each consumed user event.
// meta contains the Kafka routing context needed for idempotency checks.
type EventHandler func(ctx context.Context, meta MessageMeta, event *models.UserEvent) error

type Consumer interface {
	StartAvro(ctx context.Context, handler EventHandler) error
	StartProto(ctx context.Context, handler EventHandler) error
	Close() error
}

const retryBackoff = time.Second

// messageReader is the subset of *kafkago.Reader the consume loop needs.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

type decodeFunc func(data []byte) (*models.UserEvent, error)

// topicConsumer runs the fetch -> decode -> handle -> commit loop for one topic.
type topicConsumer struct {
	format  string
	groupID string
	reader  messageReader
	decode  decodeFunc
	backoff time.Duration
}

type consumer struct {
	avro  *topicConsumer
	proto *topicConsumer
}

// NewConsumer creates one consumer group per topic: "<groupID>-avro" and
// "<groupID>-proto". Offsets are committed manually, only after the handler succeeds.
func NewConsumer(brokers []string, groupID, schemaRegistryURL string) Consumer {
	codec := newAvroCodec(srclient.CreateSchemaRegistryClient(schemaRegistryURL))

	return &consumer{
		avro: &topicConsumer{
			format:  "avro",
			groupID: groupID + "-avro",
			reader:  newReader(brokers, groupID+"-avro", TopicUserEventsAvro),
			decode:  codec.decode,
			backoff: retryBackoff,
		},
		proto: &topicConsumer{
			format:  "proto",
			groupID: groupID + "-proto",
			reader:  newReader(brokers, groupID+"-proto", TopicUserEventsProto),
			decode:  protoCodec{}.decode,
			backoff: retryBackoff,
		},
	}
}

func newReader(brokers []string, groupID, topic string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        time.Second,
		CommitInterval: 0, // no background auto-commit; offsets are committed after handler success
	})
}

func (c *consumer) StartAvro(ctx context.Context, h EventHandler) error  { return c.avro.run(ctx, h) }
func (c *consumer) StartProto(ctx context.Context, h EventHandler) error { return c.proto.run(ctx, h) }

func (c *consumer) Close() error {
	return errors.Join(c.avro.reader.Close(), c.proto.reader.Close())
}

// run consumes until ctx is cancelled. Delivery is at-least-once: the offset is
// committed only after the handler returns nil.
//
// A message that fails to decode or handle is not committed and is skipped for
// the rest of this process (kafka-go's fetch cursor has already advanced); it is
// re-delivered after a restart from the last committed offset. Retrying in place
// or parking it in a dead-letter topic is not implemented yet.
func (t *topicConsumer) run(ctx context.Context, handler EventHandler) error {
	log := slog.With("format", t.format, "group", t.groupID)
	log.Info("kafka consumer starting")

	for {
		msg, err := t.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				continue // empty topic or closed idle connection; keep polling
			}
			log.Error("kafka fetch error", "error", err)
			if !sleep(ctx, t.backoff) {
				return ctx.Err()
			}
			continue
		}

		mlog := log.With(
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"key", string(msg.Key),
		)
		mlog.Debug("kafka fetch")

		event, err := t.decode(msg.Value)
		if err != nil {
			mlog.Error("kafka deserialize error", "error", err)
			if !sleep(ctx, t.backoff) { // avoid a hot loop on a run of bad messages
				return ctx.Err()
			}
			continue
		}

		meta := MessageMeta{
			Topic:         msg.Topic,
			Partition:     msg.Partition,
			Offset:        msg.Offset,
			ConsumerGroup: t.groupID,
		}
		if err := handler(ctx, meta, event); err != nil {
			mlog.Error("kafka handler error", "error", err)
			continue
		}

		if err := t.reader.CommitMessages(ctx, msg); err != nil {
			mlog.Error("kafka commit error", "error", err)
			continue
		}
		mlog.Info("kafka commit")
	}
}

// sleep waits for d or until ctx is done; it reports whether the full wait elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// LoggingHandler is a simple event handler that logs consumed events.
func LoggingHandler(format string) EventHandler {
	return func(ctx context.Context, meta MessageMeta, event *models.UserEvent) error {
		slog.Info("kafka event received",
			"format", format,
			"group", meta.ConsumerGroup,
			"topic", meta.Topic,
			"partition", meta.Partition,
			"offset", meta.Offset,
			"event_id", event.EventID,
			"event_type", event.EventType,
			"user_id", event.UserID,
		)
		return nil
	}
}

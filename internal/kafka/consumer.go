package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	kafkago "github.com/segmentio/kafka-go"

	"kafka-user-service/internal/models"
	pb "kafka-user-service/proto"
)

// EventHandler is called for each consumed user event
type EventHandler func(ctx context.Context, event *models.UserEvent) error

//go:generate mockery --name=Consumer --output=../mocks --outpkg=mocks
type Consumer interface {
	StartAvro(ctx context.Context, handler EventHandler) error
	StartProto(ctx context.Context, handler EventHandler) error
	Close() error
}

type consumer struct {
	avroReader   *kafkago.Reader
	protoReader  *kafkago.Reader
	avroGroupID  string
	protoGroupID string
	schemaClient srclient.ISchemaRegistryClient
}

func NewConsumer(brokers []string, groupID, schemaRegistryURL string) Consumer {
	avroGroupID := groupID + "-avro"
	protoGroupID := groupID + "-proto"

	avroReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          TopicUserEventsAvro,
		GroupID:        avroGroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        1 * time.Second,
		CommitInterval: 0, // disable background auto-commit; offsets are committed manually after handler success
	})

	protoReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          TopicUserEventsProto,
		GroupID:        protoGroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        1 * time.Second,
		CommitInterval: 0, // disable background auto-commit; offsets are committed manually after handler success
	})

	srClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	return &consumer{
		avroReader:   avroReader,
		protoReader:  protoReader,
		avroGroupID:  avroGroupID,
		protoGroupID: protoGroupID,
		schemaClient: srClient,
	}
}

func (c *consumer) StartAvro(ctx context.Context, handler EventHandler) error {
	slog.Info("kafka consumer starting", "format", "avro", "group", c.avroGroupID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.avroReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// EOF means topic is empty — suppress, just wait for messages
			if err == io.EOF || err.Error() == "fetching message: EOF" {
				continue
			}
			slog.Error("kafka fetch error",
				"format", "avro",
				"group", c.avroGroupID,
				"error", err,
			)
			continue
		}

		slog.Info("kafka fetch",
			"format", "avro",
			"group", c.avroGroupID,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"key", string(msg.Key),
		)

		event, err := c.deserializeAvro(msg.Value)
		if err != nil {
			slog.Error("kafka deserialize error",
				"format", "avro",
				"group", c.avroGroupID,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", err,
			)
			// Offset is not committed. Within this live session kafka-go's
			// fetch cursor has already advanced, so the next FetchMessage call
			// will return the following message. The failed message is only
			// re-delivered after a restart, when the broker's uncommitted offset
			// is used to resume (poison-pill until DLQ is added in Step 3).
			// A 1 s back-off prevents a tight CPU spin in the meantime.
			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
				continue
			}
			continue
		}

		if err := handler(ctx, event); err != nil {
			slog.Error("kafka handler error",
				"format", "avro",
				"group", c.avroGroupID,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", err,
			)
			// offset not committed — message will be re-read after restart
			continue
		}

		if err := c.avroReader.CommitMessages(ctx, msg); err != nil {
			slog.Error("kafka commit error",
				"format", "avro",
				"group", c.avroGroupID,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", err,
			)
			// The broker's committed offset has not moved. The message will be
			// re-delivered after a restart (at-least-once). Within this live
			// session the reader's internal fetch position has already advanced,
			// so the message will not be retried until the next process start.
			continue
		}

		slog.Info("kafka commit",
			"format", "avro",
			"group", c.avroGroupID,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"key", string(msg.Key),
		)
	}
}

func (c *consumer) StartProto(ctx context.Context, handler EventHandler) error {
	slog.Info("kafka consumer starting", "format", "proto", "group", c.protoGroupID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.protoReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// EOF means topic is empty — suppress, just wait for messages
			if err == io.EOF || err.Error() == "fetching message: EOF" {
				continue
			}
			slog.Error("kafka fetch error",
				"format", "proto",
				"group", c.protoGroupID,
				"error", err,
			)
			continue
		}

		slog.Info("kafka fetch",
			"format", "proto",
			"group", c.protoGroupID,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"key", string(msg.Key),
		)

		event, err := c.deserializeProto(msg.Value)
		if err != nil {
			slog.Error("kafka deserialize error",
				"format", "proto",
				"group", c.protoGroupID,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", err,
			)
			// Offset is not committed. Within this live session kafka-go's
			// fetch cursor has already advanced, so the next FetchMessage call
			// will return the following message. The failed message is only
			// re-delivered after a restart, when the broker's uncommitted offset
			// is used to resume (poison-pill until DLQ is added in Step 3).
			// A 1 s back-off prevents a tight CPU spin in the meantime.
			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
				continue
			}
			continue
		}

		if err := handler(ctx, event); err != nil {
			slog.Error("kafka handler error",
				"format", "proto",
				"group", c.protoGroupID,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", err,
			)
			// offset not committed — message will be re-read after restart
			continue
		}

		if err := c.protoReader.CommitMessages(ctx, msg); err != nil {
			slog.Error("kafka commit error",
				"format", "proto",
				"group", c.protoGroupID,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", err,
			)
			// The broker's committed offset has not moved. The message will be
			// re-delivered after a restart (at-least-once). Within this live
			// session the reader's internal fetch position has already advanced,
			// so the message will not be retried until the next process start.
			continue
		}

		slog.Info("kafka commit",
			"format", "proto",
			"group", c.protoGroupID,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"key", string(msg.Key),
		)
	}
}

func (c *consumer) deserializeAvro(data []byte) (*models.UserEvent, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("invalid confluent wire format: too short")
	}
	if data[0] != MagicByte {
		return nil, fmt.Errorf("invalid magic byte")
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	schema, err := c.schemaClient.GetSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("fetch schema %d: %w", schemaID, err)
	}

	parsedSchema, err := avro.Parse(schema.Schema())
	if err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}

	var native map[string]interface{}
	if err := avro.Unmarshal(parsedSchema, data[5:], &native); err != nil {
		return nil, fmt.Errorf("avro unmarshal: %w", err)
	}

	tsMillis, _ := native["timestamp"].(int64)

	// hamba/avro decodes Avro int as int32
	var age int
	switch v := native["age"].(type) {
	case int32:
		age = int(v)
	case int64:
		age = int(v)
	case int:
		age = v
	}

	return &models.UserEvent{
		EventType: native["event_type"].(string),
		UserID:    native["user_id"].(string),
		Name:      native["name"].(string),
		Email:     native["email"].(string),
		Age:       age,
		Timestamp: time.UnixMilli(tsMillis),
	}, nil
}

// deserializeProto decodes a Confluent-prefixed JSON payload into a UserEvent.
func (c *consumer) deserializeProto(data []byte) (*models.UserEvent, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("invalid confluent wire format: too short")
	}
	if data[0] != MagicByte {
		return nil, fmt.Errorf("invalid magic byte")
	}

	var pbEvent pb.UserEvent
	if err := json.Unmarshal(data[5:], &pbEvent); err != nil {
		return nil, fmt.Errorf("proto json unmarshal: %w", err)
	}

	return &models.UserEvent{
		EventType: pbEvent.EventType.String(),
		UserID:    pbEvent.UserId,
		Name:      pbEvent.Name,
		Email:     pbEvent.Email,
		Age:       pbEvent.Age,
		Timestamp: pbEvent.Timestamp,
	}, nil
}

func (c *consumer) Close() error {
	return errors.Join(c.avroReader.Close(), c.protoReader.Close())
}

// LoggingHandler is a simple event handler that logs consumed events
func LoggingHandler(format string) EventHandler {
	return func(ctx context.Context, event *models.UserEvent) error {
		b, _ := json.MarshalIndent(event, "", "  ")
		slog.Info("kafka event received", "format", format, "event", string(b))
		return nil
	}
}

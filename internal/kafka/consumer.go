package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	schemaClient srclient.ISchemaRegistryClient
}

func NewConsumer(brokers []string, groupID, schemaRegistryURL string) Consumer {
	avroReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		Topic:    TopicUserEventsAvro,
		GroupID:  groupID + "-avro",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})

	protoReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		Topic:    TopicUserEventsProto,
		GroupID:  groupID + "-proto",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})

	srClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	return &consumer{
		avroReader:   avroReader,
		protoReader:  protoReader,
		schemaClient: srClient,
	}
}

func (c *consumer) StartAvro(ctx context.Context, handler EventHandler) error {
	log.Println("[Consumer] Starting Avro consumer...")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.avroReader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// EOF means topic is empty — suppress, just wait for messages
			if err == io.EOF || err.Error() == "fetching message: EOF" {
				continue
			}
			log.Printf("[Consumer] Avro read error: %v", err)
			continue
		}

		event, err := c.deserializeAvro(msg.Value)
		if err != nil {
			log.Printf("[Consumer] Avro deserialize error: %v", err)
			continue
		}

		if err := handler(ctx, event); err != nil {
			log.Printf("[Consumer] Avro handler error: %v", err)
		}
	}
}

func (c *consumer) StartProto(ctx context.Context, handler EventHandler) error {
	log.Println("[Consumer] Starting Protobuf consumer...")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.protoReader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// EOF means topic is empty — suppress, just wait for messages
			if err == io.EOF || err.Error() == "fetching message: EOF" {
				continue
			}
			log.Printf("[Consumer] Proto read error: %v", err)
			continue
		}

		event, err := c.deserializeProto(msg.Value)
		if err != nil {
			log.Printf("[Consumer] Proto deserialize error: %v", err)
			continue
		}

		if err := handler(ctx, event); err != nil {
			log.Printf("[Consumer] Proto handler error: %v", err)
		}
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
	if err := c.avroReader.Close(); err != nil {
		return err
	}
	return c.protoReader.Close()
}

// LoggingHandler is a simple event handler that logs consumed events
func LoggingHandler(format string) EventHandler {
	return func(ctx context.Context, event *models.UserEvent) error {
		b, _ := json.MarshalIndent(event, "", "  ")
		log.Printf("[Consumer][%s] Received event:\n%s", format, string(b))
		return nil
	}
}

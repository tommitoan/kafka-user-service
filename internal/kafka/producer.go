package kafka

import (
	"context"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/tommitoan/kafka-user-service/internal/models"
	pb "github.com/tommitoan/kafka-user-service/proto"
	"github.com/tommitoan/kafka-user-service/schemas"
)

// Producer publishes each user event to both the Avro and the Protobuf topic.
type Producer interface {
	PublishUserEvent(ctx context.Context, event *models.UserEvent) error
	Close() error
}

type producer struct {
	avroWriter  *kafkago.Writer
	protoWriter *kafkago.Writer
	avro        *avroCodec
	proto       protoCodec
}

// NewProducer registers the Avro and Protobuf schemas in Schema Registry and
// returns a producer that encodes events with the resulting schema IDs.
func NewProducer(brokers []string, schemaRegistryURL string) (Producer, error) {
	return newProducer(brokers, srclient.CreateSchemaRegistryClient(schemaRegistryURL))
}

func newProducer(brokers []string, registry srclient.ISchemaRegistryClient) (*producer, error) {
	avroSchema, err := registry.CreateSchema(TopicUserEventsAvro+"-value", schemas.UserEventAvro, srclient.Avro)
	if err != nil {
		return nil, fmt.Errorf("register avro schema: %w", err)
	}
	parsedAvro, err := avro.Parse(schemas.UserEventAvro)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}

	protoSchema, err := registry.CreateSchema(TopicUserEventsProto+"-value", pb.Schema, srclient.Protobuf)
	if err != nil {
		return nil, fmt.Errorf("register protobuf schema: %w", err)
	}

	ac := newAvroCodec(registry)
	ac.producerSchema = parsedAvro
	ac.producerID = avroSchema.ID()

	return &producer{
		avroWriter:  newWriter(brokers, TopicUserEventsAvro),
		protoWriter: newWriter(brokers, TopicUserEventsProto),
		avro:        ac,
		proto:       protoCodec{schemaID: protoSchema.ID()},
	}, nil
}

func newWriter(brokers []string, topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{}, // same user -> same partition -> per-user ordering
		RequiredAcks: kafkago.RequireAll,
	}
}

// PublishUserEvent writes the event to both topics. The two writes are not atomic:
// if the second fails the first has already been delivered. Consumers dedupe on
// event_id, so callers may safely retry the whole call.
func (p *producer) PublishUserEvent(ctx context.Context, event *models.UserEvent) error {
	avroPayload, err := p.avro.encode(event)
	if err != nil {
		return fmt.Errorf("avro serialize: %w", err)
	}
	protoPayload, err := p.proto.encode(event)
	if err != nil {
		return fmt.Errorf("proto serialize: %w", err)
	}

	key := []byte(event.UserID)
	if err := p.avroWriter.WriteMessages(ctx, kafkago.Message{Key: key, Value: avroPayload}); err != nil {
		return fmt.Errorf("write avro message: %w", err)
	}
	if err := p.protoWriter.WriteMessages(ctx, kafkago.Message{Key: key, Value: protoPayload}); err != nil {
		return fmt.Errorf("write proto message: %w", err)
	}
	return nil
}

func (p *producer) Close() error {
	avroErr := p.avroWriter.Close()
	protoErr := p.protoWriter.Close()
	if avroErr != nil {
		return avroErr
	}
	return protoErr
}

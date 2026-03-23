package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	kafkago "github.com/segmentio/kafka-go"

	"kafka-user-service/internal/models"
	pb "kafka-user-service/proto"
)

const (
	MagicByte = byte(0) // Confluent Schema Registry wire format magic byte
)

//go:generate mockery --name=Producer --output=../mocks --outpkg=mocks
type Producer interface {
	PublishUserEvent(ctx context.Context, event *models.UserEvent) error
	Close() error
}

type producer struct {
	avroWriter    *kafkago.Writer
	protoWriter   *kafkago.Writer
	schemaClient  srclient.ISchemaRegistryClient
	avroSchema    avro.Schema
	avroSchemaID  int
	protoSchemaID int
}

func NewProducer(brokers []string, schemaRegistryURL string) (Producer, error) {
	srClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	// Register Avro schema
	avroSchemaStr := `{
		"type": "record",
		"name": "UserEvent",
		"namespace": "com.example.user",
		"fields": [
			{"name": "event_type", "type": "string"},
			{"name": "user_id",    "type": "string"},
			{"name": "name",       "type": "string"},
			{"name": "email",      "type": "string"},
			{"name": "age",        "type": "int"},
			{"name": "timestamp",  "type": {"type": "long", "logicalType": "timestamp-millis"}}
		]
	}`

	avroSch, err := srClient.CreateSchema(TopicUserEventsAvro+"-value", avroSchemaStr, srclient.Avro)
	if err != nil {
		return nil, fmt.Errorf("register avro schema: %w", err)
	}

	// Register Protobuf schema (stored in registry for governance; serialized as JSON)
	protoSchemaStr := `syntax = "proto3";
package com.example.user;
import "google/protobuf/timestamp.proto";
enum EventType { CREATED = 0; UPDATED = 1; DELETED = 2; }
message UserEvent {
  EventType event_type = 1; string user_id = 2; string name = 3;
  string email = 4; int32 age = 5; google.protobuf.Timestamp timestamp = 6;
}`

	protoSch, err := srClient.CreateSchema(TopicUserEventsProto+"-value", protoSchemaStr, srclient.Protobuf)
	if err != nil {
		return nil, fmt.Errorf("register protobuf schema: %w", err)
	}

	parsedAvro, err := avro.Parse(avroSchemaStr)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}

	avroWriter := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        TopicUserEventsAvro,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireOne,
	}

	protoWriter := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        TopicUserEventsProto,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireOne,
	}

	return &producer{
		avroWriter:    avroWriter,
		protoWriter:   protoWriter,
		schemaClient:  srClient,
		avroSchema:    parsedAvro,
		avroSchemaID:  avroSch.ID(),
		protoSchemaID: protoSch.ID(),
	}, nil
}

func (p *producer) PublishUserEvent(ctx context.Context, event *models.UserEvent) error {
	avroPayload, err := p.serializeAvro(event)
	if err != nil {
		return fmt.Errorf("avro serialize: %w", err)
	}

	protoPayload, err := p.serializeProto(event)
	if err != nil {
		return fmt.Errorf("proto serialize: %w", err)
	}

	key := []byte(event.UserID)

	if err := p.avroWriter.WriteMessages(ctx, kafkago.Message{
		Key:   key,
		Value: avroPayload,
	}); err != nil {
		return fmt.Errorf("write avro message: %w", err)
	}

	if err := p.protoWriter.WriteMessages(ctx, kafkago.Message{
		Key:   key,
		Value: protoPayload,
	}); err != nil {
		return fmt.Errorf("write proto message: %w", err)
	}

	return nil
}

// serializeAvro encodes using Confluent wire format:
// [magic byte (0x00)] [4-byte schema ID big-endian] [avro bytes]
func (p *producer) serializeAvro(event *models.UserEvent) ([]byte, error) {
	native := map[string]interface{}{
		"event_type": string(event.EventType),
		"user_id":    event.UserID,
		"name":       event.Name,
		"email":      event.Email,
		"age":        event.Age,
		"timestamp":  event.Timestamp.UnixMilli(),
	}

	avroBytes, err := avro.Marshal(p.avroSchema, native)
	if err != nil {
		return nil, err
	}

	payload := make([]byte, 5+len(avroBytes))
	payload[0] = MagicByte
	binary.BigEndian.PutUint32(payload[1:5], uint32(p.avroSchemaID))
	copy(payload[5:], avroBytes)
	return payload, nil
}

// serializeProto encodes using JSON + Confluent wire format prefix.
// (No protoc dependency — schema is registered in Schema Registry for governance.)
func (p *producer) serializeProto(event *models.UserEvent) ([]byte, error) {
	var evtType pb.EventType
	switch models.EventType(event.EventType) {
	case models.EventCreated:
		evtType = pb.EventType_CREATED
	case models.EventUpdated:
		evtType = pb.EventType_UPDATED
	case models.EventDeleted:
		evtType = pb.EventType_DELETED
	}

	pbEvent := &pb.UserEvent{
		EventType: evtType,
		UserId:    event.UserID,
		Name:      event.Name,
		Email:     event.Email,
		Age:       event.Age,
		Timestamp: event.Timestamp,
	}

	jsonBytes, err := json.Marshal(pbEvent)
	if err != nil {
		return nil, err
	}

	payload := make([]byte, 5+len(jsonBytes))
	payload[0] = MagicByte
	binary.BigEndian.PutUint32(payload[1:5], uint32(p.protoSchemaID))
	copy(payload[5:], jsonBytes)
	return payload, nil
}

func (p *producer) Close() error {
	if err := p.avroWriter.Close(); err != nil {
		return err
	}
	return p.protoWriter.Close()
}

// -- Fallback JSON producer for testing without Schema Registry --

type jsonProducer struct {
	writer *kafkago.Writer
}

func NewJSONProducer(brokers []string, topic string) Producer {
	return &jsonProducer{
		writer: &kafkago.Writer{
			Addr:     kafkago.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafkago.LeastBytes{},
		},
	}
}

func (p *jsonProducer) PublishUserEvent(ctx context.Context, event *models.UserEvent) error {
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(event.UserID),
		Value: b,
		Time:  time.Now(),
	})
}

func (p *jsonProducer) Close() error {
	return p.writer.Close()
}

package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tommitoan/kafka-user-service/internal/models"
	pb "github.com/tommitoan/kafka-user-service/proto"
)

// Confluent wire format: [magic byte 0x00][4-byte big-endian schema ID][payload].
// Protobuf payloads additionally carry a message-index array between the schema
// ID and the serialized message; UserEvent is the first (and only) message in
// its file, which Confluent encodes as the single byte 0x00.
const (
	MagicByte = byte(0)

	headerLen        = 5
	protoMessageIdx0 = byte(0)
)

var errBadFrame = errors.New("invalid confluent wire format")

func frame(schemaID int, indexes []byte, payload []byte) []byte {
	out := make([]byte, headerLen+len(indexes)+len(payload))
	out[0] = MagicByte
	binary.BigEndian.PutUint32(out[1:headerLen], uint32(schemaID))
	copy(out[headerLen:], indexes)
	copy(out[headerLen+len(indexes):], payload)
	return out
}

// unframe validates the header and returns the schema ID and the remaining bytes.
func unframe(data []byte) (schemaID int, rest []byte, err error) {
	if len(data) < headerLen {
		return 0, nil, fmt.Errorf("%w: too short", errBadFrame)
	}
	if data[0] != MagicByte {
		return 0, nil, fmt.Errorf("%w: unexpected magic byte %#x", errBadFrame, data[0])
	}
	return int(binary.BigEndian.Uint32(data[1:headerLen])), data[headerLen:], nil
}

// avroUserEvent is the Go shape of schemas/avro/user_event.avsc.
type avroUserEvent struct {
	EventType string    `avro:"event_type"`
	UserID    string    `avro:"user_id"`
	Name      string    `avro:"name"`
	Email     string    `avro:"email"`
	Age       int       `avro:"age"`
	Timestamp time.Time `avro:"timestamp"`
	EventID   string    `avro:"event_id"`
}

type avroCodec struct {
	producerSchema avro.Schema // used when encoding
	producerID     int
	registry       srclient.ISchemaRegistryClient

	mu      sync.RWMutex
	schemas map[int]avro.Schema // writer schemas by registry ID, used when decoding
}

func newAvroCodec(registry srclient.ISchemaRegistryClient) *avroCodec {
	return &avroCodec{registry: registry, schemas: make(map[int]avro.Schema)}
}

func (c *avroCodec) encode(e *models.UserEvent) ([]byte, error) {
	payload, err := avro.Marshal(c.producerSchema, avroUserEvent{
		EventType: e.EventType,
		UserID:    e.UserID,
		Name:      e.Name,
		Email:     e.Email,
		Age:       e.Age,
		Timestamp: e.Timestamp,
		EventID:   e.EventID,
	})
	if err != nil {
		return nil, err
	}
	return frame(c.producerID, nil, payload), nil
}

func (c *avroCodec) decode(data []byte) (*models.UserEvent, error) {
	schemaID, payload, err := unframe(data)
	if err != nil {
		return nil, err
	}
	schema, err := c.writerSchema(schemaID)
	if err != nil {
		return nil, err
	}

	var in avroUserEvent
	if err := avro.Unmarshal(schema, payload, &in); err != nil {
		return nil, fmt.Errorf("avro unmarshal: %w", err)
	}
	return &models.UserEvent{
		EventID:   in.EventID,
		EventType: in.EventType,
		UserID:    in.UserID,
		Name:      in.Name,
		Email:     in.Email,
		Age:       in.Age,
		Timestamp: in.Timestamp,
	}, nil
}

// writerSchema resolves and caches the parsed schema for a registry ID.
func (c *avroCodec) writerSchema(id int) (avro.Schema, error) {
	c.mu.RLock()
	s, ok := c.schemas[id]
	c.mu.RUnlock()
	if ok {
		return s, nil
	}

	registered, err := c.registry.GetSchema(id)
	if err != nil {
		return nil, fmt.Errorf("fetch schema %d: %w", id, err)
	}
	s, err = avro.Parse(registered.Schema())
	if err != nil {
		return nil, fmt.Errorf("parse avro schema %d: %w", id, err)
	}

	c.mu.Lock()
	c.schemas[id] = s
	c.mu.Unlock()
	return s, nil
}

type protoCodec struct {
	schemaID int
}

func (c protoCodec) encode(e *models.UserEvent) ([]byte, error) {
	evtType, ok := pb.EventType_value[e.EventType]
	if !ok {
		return nil, fmt.Errorf("unknown event type %q", e.EventType)
	}
	payload, err := proto.Marshal(&pb.UserEvent{
		EventType: pb.EventType(evtType),
		UserId:    e.UserID,
		Name:      e.Name,
		Email:     e.Email,
		Age:       int32(e.Age),
		Timestamp: timestamppb.New(e.Timestamp),
		EventId:   e.EventID,
	})
	if err != nil {
		return nil, err
	}
	return frame(c.schemaID, []byte{protoMessageIdx0}, payload), nil
}

func (c protoCodec) decode(data []byte) (*models.UserEvent, error) {
	_, payload, err := unframe(data)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 || payload[0] != protoMessageIdx0 {
		return nil, fmt.Errorf("%w: unsupported protobuf message index", errBadFrame)
	}

	var in pb.UserEvent
	if err := proto.Unmarshal(payload[1:], &in); err != nil {
		return nil, fmt.Errorf("proto unmarshal: %w", err)
	}
	return &models.UserEvent{
		EventID:   in.EventId,
		EventType: in.EventType.String(),
		UserID:    in.UserId,
		Name:      in.Name,
		Email:     in.Email,
		Age:       int(in.Age),
		Timestamp: in.Timestamp.AsTime(),
	}, nil
}

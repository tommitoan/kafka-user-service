package kafka

import (
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tommitoan/kafka-user-service/internal/models"
	pb "github.com/tommitoan/kafka-user-service/proto"
	"github.com/tommitoan/kafka-user-service/schemas"
)

func sampleEvent() *models.UserEvent {
	return &models.UserEvent{
		EventID:   "7b6e6f0a-1c52-4a53-8c5e-2f3b0a8f9d11",
		EventType: string(models.EventUpdated),
		UserID:    "0f8fad5b-d9cb-469f-a165-70867728950e",
		Name:      "Alice",
		Email:     "alice@example.com",
		Age:       30,
		// Millisecond precision: Avro timestamp-millis cannot carry more.
		Timestamp: time.UnixMilli(1_760_000_000_123).UTC(),
	}
}

func newTestProducer(t *testing.T) (*producer, srclient.ISchemaRegistryClient) {
	t.Helper()
	registry := srclient.CreateMockSchemaRegistryClient("mock://codec-test")
	p, err := newProducer([]string{"localhost:9092"}, registry)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return p, registry
}

func TestAvroCodec_RoundTrip(t *testing.T) {
	p, registry := newTestProducer(t)

	data, err := p.avro.encode(sampleEvent())
	require.NoError(t, err)

	// The consumer side resolves the writer schema from the registry by ID.
	got, err := newAvroCodec(registry).decode(data)
	require.NoError(t, err)
	assert.Equal(t, sampleEvent(), got)
}

func TestAvroCodec_FramesWithSchemaID(t *testing.T) {
	p, _ := newTestProducer(t)

	data, err := p.avro.encode(sampleEvent())
	require.NoError(t, err)

	id, _, err := unframe(data)
	require.NoError(t, err)
	assert.Equal(t, p.avro.producerID, id)
}

// Messages written before event_id existed use a schema without that field and
// must still decode, with an empty EventID.
func TestAvroCodec_DecodesWriterSchemaWithoutEventID(t *testing.T) {
	registry := srclient.CreateMockSchemaRegistryClient("mock://legacy")
	legacy := `{"type":"record","name":"UserEvent","namespace":"com.example.user","fields":[
		{"name":"event_type","type":"string"},{"name":"user_id","type":"string"},
		{"name":"name","type":"string"},{"name":"email","type":"string"},
		{"name":"age","type":"int"},
		{"name":"timestamp","type":{"type":"long","logicalType":"timestamp-millis"}}]}`
	registered, err := registry.CreateSchema("legacy-value", legacy, srclient.Avro)
	require.NoError(t, err)

	schema, err := avro.Parse(legacy)
	require.NoError(t, err)
	payload, err := avro.Marshal(schema, map[string]any{
		"event_type": "CREATED", "user_id": "u1", "name": "Bob", "email": "bob@example.com",
		"age": 41, "timestamp": time.UnixMilli(1_700_000_000_000),
	})
	require.NoError(t, err)

	got, err := newAvroCodec(registry).decode(frame(registered.ID(), nil, payload))
	require.NoError(t, err)
	assert.Empty(t, got.EventID)
	assert.Equal(t, "Bob", got.Name)
	assert.Equal(t, 41, got.Age)
}

func TestProtoCodec_RoundTrip(t *testing.T) {
	p, _ := newTestProducer(t)

	data, err := p.proto.encode(sampleEvent())
	require.NoError(t, err)

	got, err := protoCodec{}.decode(data)
	require.NoError(t, err)
	assert.Equal(t, sampleEvent(), got)
}

// The payload must be real Protobuf (not JSON) in Confluent's framing:
// magic byte, schema ID, message-index array [0] encoded as one 0x00 byte.
func TestProtoCodec_UsesProtobufWireFormat(t *testing.T) {
	p, _ := newTestProducer(t)

	data, err := p.proto.encode(sampleEvent())
	require.NoError(t, err)

	id, rest, err := unframe(data)
	require.NoError(t, err)
	assert.Equal(t, p.proto.schemaID, id)
	require.NotEmpty(t, rest)
	assert.Equal(t, protoMessageIdx0, rest[0])

	var decoded pb.UserEvent
	require.NoError(t, proto.Unmarshal(rest[1:], &decoded))
	assert.Equal(t, sampleEvent().EventID, decoded.EventId)
	assert.Equal(t, pb.EventType_UPDATED, decoded.EventType)
	assert.NotEqual(t, byte('{'), rest[1], "payload must not be JSON")
}

func TestProtoCodec_RejectsUnknownEventType(t *testing.T) {
	e := sampleEvent()
	e.EventType = "EXPLODED"
	_, err := protoCodec{}.encode(e)
	assert.Error(t, err)
}

func TestUnframe_RejectsMalformedFrames(t *testing.T) {
	tests := map[string][]byte{
		"empty":        nil,
		"too short":    {0, 0, 0, 0},
		"bad magic":    {1, 0, 0, 0, 1, 42},
		"json payload": []byte(`{"event_id":"x"}`),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := unframe(data)
			assert.ErrorIs(t, err, errBadFrame)
		})
	}
}

func TestProtoCodec_DecodeRejectsGarbage(t *testing.T) {
	_, err := protoCodec{}.decode(frame(1, []byte{protoMessageIdx0}, []byte{0xff, 0xff, 0xff}))
	assert.Error(t, err)

	_, err = protoCodec{}.decode(frame(1, []byte{7}, nil))
	assert.ErrorIs(t, err, errBadFrame)
}

// The embedded schemas are the registered contract; guard against them drifting
// from the Go types the codecs use.
func TestEmbeddedSchemasParse(t *testing.T) {
	_, err := avro.Parse(schemas.UserEventAvro)
	assert.NoError(t, err)
	assert.Contains(t, pb.Schema, "message UserEvent")
	assert.Contains(t, pb.Schema, "event_id")
}

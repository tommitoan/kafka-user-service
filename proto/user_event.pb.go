// !! DO NOT REGENERATE with protoc. This file is hand-maintained. !!
// !! Running `protoc` or `go generate` will overwrite EventId and        !!
// !! break consumer-side idempotency at runtime with no compile error.   !!
// !! To intentionally regenerate, read the Makefile proto-gen target     !!
// !! and manually re-add the EventId field and this header afterwards.   !!
//
// Hand-written protobuf-compatible Go struct.
// Since we are not running protoc, we use encoding/json for
// serialization instead of google.golang.org/protobuf/proto.
// The wire format is still Confluent-prefixed (magic + schema ID + payload).

package proto

import "time"

type EventType int32

const (
	EventType_CREATED EventType = 0
	EventType_UPDATED EventType = 1
	EventType_DELETED EventType = 2
)

func (e EventType) String() string {
	switch e {
	case EventType_CREATED:
		return "CREATED"
	case EventType_UPDATED:
		return "UPDATED"
	case EventType_DELETED:
		return "DELETED"
	default:
		return "UNKNOWN"
	}
}

// UserEvent is the Protobuf-schema-compatible event struct.
// Serialized with encoding/json for simplicity (no protoc required).
// EventId (field 7) is the stable dedup key generated once by the producer.
type UserEvent struct {
	EventType EventType `json:"event_type"`
	UserId    string    `json:"user_id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Age       int       `json:"age"`
	Timestamp time.Time `json:"timestamp"`
	EventId   string    `json:"event_id"`
}

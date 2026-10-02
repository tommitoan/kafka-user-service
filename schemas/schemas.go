// Package schemas embeds the event schemas that are registered in Schema Registry,
// so the .avsc file is the single source of truth for the Avro contract.
package schemas

import _ "embed"

// UserEventAvro is the Avro schema for user events (schemas/avro/user_event.avsc).
//
//go:embed avro/user_event.avsc
var UserEventAvro string

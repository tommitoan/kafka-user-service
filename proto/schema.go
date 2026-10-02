package proto

import _ "embed"

// Schema is the Protobuf source registered in Schema Registry; it is the same
// file the Go types in user_event.pb.go are generated from.
//
//go:embed user_event.proto
var Schema string

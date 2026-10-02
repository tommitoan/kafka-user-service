package kafka

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"

	kafkago "github.com/segmentio/kafka-go"
)

const (
	TopicUserEventsAvro  = "com.br4.user.core.event.avro"
	TopicUserEventsProto = "com.br4.user.core.event.proto"
)

// TopicDefinition is passed in from config.
type TopicDefinition struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
}

// EnsureTopics creates all declared Kafka topics if they do not already exist.
// Driven entirely by config.yaml — adding a new topic requires no code changes.
// Safe to call on every startup.
func EnsureTopics(brokers []string, topics []TopicDefinition) error {
	if len(topics) == 0 {
		slog.Info("no kafka topics declared in config, skipping creation")
		return nil
	}

	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	// Topic creation must go to the controller
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafkago.Dial("tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("dial controller %s: %w", controllerAddr, err)
	}
	defer controllerConn.Close()

	// First check which topics already exist so we only create missing ones
	existingTopics, err := existingTopicSet(conn)
	if err != nil {
		// Non-fatal — just attempt to create all and ignore already-exists errors
		slog.Warn("could not list existing kafka topics, attempting create anyway", "error", err)
		existingTopics = map[string]struct{}{}
	}

	var toCreate []kafkago.TopicConfig
	for _, t := range topics {
		if _, exists := existingTopics[t.Name]; exists {
			slog.Info("kafka topic already exists", "topic", t.Name)
			continue
		}
		toCreate = append(toCreate, kafkago.TopicConfig{
			Topic:             t.Name,
			NumPartitions:     t.NumPartitions,
			ReplicationFactor: t.ReplicationFactor,
		})
	}

	if len(toCreate) > 0 {
		err = controllerConn.CreateTopics(toCreate...)
		if err != nil && !isIgnorableError(err) {
			return fmt.Errorf("create topics: %w", err)
		}
		for _, t := range toCreate {
			slog.Info("kafka topic created",
				"topic", t.Topic, "partitions", t.NumPartitions, "replication", t.ReplicationFactor)
		}
	}

	return nil
}

// existingTopicSet returns a set of topic names currently in the cluster.
func existingTopicSet(conn *kafkago.Conn) (map[string]struct{}, error) {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}
	topics := make(map[string]struct{})
	for _, p := range partitions {
		topics[p.Topic] = struct{}{}
	}
	return topics, nil
}

// isIgnorableError returns true for errors that mean the topic already exists
// or that Confluent Kafka returns spuriously on success (EOF).
func isIgnorableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "topic already exists") ||
		strings.Contains(msg, "topic_already_exists") ||
		strings.Contains(msg, "eof")
}

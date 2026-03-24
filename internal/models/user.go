package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type User struct {
	ID        uuid.UUID      `gorm:"type:uuid;primaryKey" json:"id"`
	Name      string         `gorm:"not null" json:"name"`
	Email     string         `gorm:"uniqueIndex;not null" json:"email"`
	Age       int            `json:"age"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

func (u *User) BeforeCreate(tx *gorm.DB) error {
	if u.ID == uuid.Nil {
		u.ID = uuid.New()
	}
	return nil
}

// UserEvent represents a user snapshot event published to Kafka.
// EventID is a stable UUID generated once by the producer; it survives
// Kafka at-least-once redelivery and is used for consumer-side idempotency.
type UserEvent struct {
	EventID   string    `json:"event_id"`   // stable dedup key, UUID
	EventType string    `json:"event_type"` // CREATED, UPDATED, DELETED
	UserID    string    `json:"user_id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Age       int       `json:"age"`
	Timestamp time.Time `json:"timestamp"`
}

type EventType string

const (
	EventCreated EventType = "CREATED"
	EventUpdated EventType = "UPDATED"
	EventDeleted EventType = "DELETED"
)

// ProcessedEvent records that a specific event has been handled by a consumer group
// on a specific topic. The composite primary key (consumer_group, topic, event_id)
// ensures the Avro and Proto flows remain independent: the same logical event
// published to two topics can be processed by each consumer group exactly once.
//
// Field order matches the SQL migration PK: (consumer_group, topic, event_id).
// GORM AutoMigrate derives PK column order from struct field order; keeping this
// aligned with 000002_create_processed_events_table.up.sql avoids index skew
// between test (AutoMigrate) and production (go-migrate) environments.
type ProcessedEvent struct {
	ConsumerGroup  string    `gorm:"primaryKey;column:consumer_group"`
	Topic          string    `gorm:"primaryKey;column:topic"`
	EventID        string    `gorm:"primaryKey;column:event_id"`
	KafkaPartition int       `gorm:"column:kafka_partition"`
	KafkaOffset    int64     `gorm:"column:kafka_offset"`
	ProcessedAt    time.Time `gorm:"column:processed_at;autoCreateTime"`
}

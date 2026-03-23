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

// UserEvent represents a user snapshot event published to Kafka
type UserEvent struct {
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

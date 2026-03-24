package repository

import (
	"context"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"kafka-user-service/internal/models"
)

// ProcessedEventRepository persists deduplication markers for consumed Kafka events.
// The uniqueness contract is (consumer_group, topic, event_id): the same logical
// event published to both the Avro and Proto topics is handled independently by
// each consumer group, so neither flow blocks the other.
//
//go:generate mockery --name=ProcessedEventRepository --output=../mocks --outpkg=mocks
type ProcessedEventRepository interface {
	// RecordIfNotExists inserts a processed_events row.
	//
	// Returns (true, nil)  — row already existed (duplicate delivery).
	// Returns (false, nil) — row was freshly inserted (first delivery).
	// Returns (_, err)     — DB error; caller should not commit the Kafka offset.
	RecordIfNotExists(ctx context.Context, pe models.ProcessedEvent) (alreadyProcessed bool, err error)
}

type processedEventRepository struct {
	db *gorm.DB
}

func NewProcessedEventRepository(db *gorm.DB) ProcessedEventRepository {
	return &processedEventRepository{db: db}
}

// RecordIfNotExists performs an atomic INSERT … ON CONFLICT DO NOTHING.
// When the row already exists (conflict on the composite PK), RowsAffected is 0
// and the function returns alreadyProcessed=true without touching the existing row.
func (r *processedEventRepository) RecordIfNotExists(
	ctx context.Context,
	pe models.ProcessedEvent,
) (bool, error) {
	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&pe)

	if result.Error != nil {
		return false, result.Error
	}

	// RowsAffected == 0 means the conflict clause fired — row already existed.
	return result.RowsAffected == 0, nil
}

package kafka

import (
	"context"
	"log/slog"

	"gorm.io/gorm"

	"kafka-user-service/internal/models"
	"kafka-user-service/internal/repository"
)

// NewIdempotentHandler wraps an inner EventHandler with consumer-side deduplication.
//
// The dedup check and the inner handler call share a single DB transaction so that
// a failure inside inner() rolls back the processed_events row. On the next delivery
// (after a restart) the message will be retried correctly — the row will be absent and
// inner() will run again.
//
// Transaction contract for each message:
//  1. If event_id is empty (pre-migration message): warn, fall through to inner, no DB write.
//  2. BEGIN TRANSACTION
//  3. INSERT INTO processed_events … ON CONFLICT DO NOTHING
//  4. If RowsAffected == 0 (duplicate): COMMIT transaction (no-op), log skip, return nil.
//     Caller commits the Kafka offset.
//  5. If fresh row inserted: call inner().
//     - inner() error  → ROLLBACK transaction (row removed), return error.
//     Caller does NOT commit offset; message is re-delivered on restart.
//     - inner() success → COMMIT transaction, return nil.
//     Caller commits offset.
//
// The inner handler is called exactly once per unique (consumer_group, topic, event_id).
func NewIdempotentHandler(db *gorm.DB, inner EventHandler) EventHandler {
	return func(ctx context.Context, meta MessageMeta, event *models.UserEvent) error {
		if event.EventID == "" {
			slog.Warn("kafka event missing event_id — idempotency check skipped",
				"group", meta.ConsumerGroup,
				"topic", meta.Topic,
				"partition", meta.Partition,
				"offset", meta.Offset,
			)
			return inner(ctx, meta, event)
		}

		pe := models.ProcessedEvent{
			EventID:        event.EventID,
			ConsumerGroup:  meta.ConsumerGroup,
			Topic:          meta.Topic,
			KafkaPartition: meta.Partition,
			KafkaOffset:    meta.Offset,
		}

		var isDuplicate bool

		// Both the dedup marker INSERT and the inner handler run inside one transaction.
		// If inner() fails the INSERT is rolled back; the event will be retried on restart.
		txErr := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			txRepo := repository.NewProcessedEventRepository(tx)

			alreadyProcessed, err := txRepo.RecordIfNotExists(ctx, pe)
			if err != nil {
				return err
			}
			if alreadyProcessed {
				isDuplicate = true
				return nil // COMMIT (no rows written; commit is a no-op)
			}

			return inner(ctx, meta, event) // COMMIT on nil, ROLLBACK on error
		})

		if txErr != nil {
			// inner() or the DB write failed; caller will not commit the offset.
			return txErr
		}

		if isDuplicate {
			slog.Info("kafka duplicate event skipped",
				"event_id", event.EventID,
				"consumer_group", meta.ConsumerGroup,
				"topic", meta.Topic,
				"partition", meta.Partition,
				"offset", meta.Offset,
			)
		}

		return nil
	}
}

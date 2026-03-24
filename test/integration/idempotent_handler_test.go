//go:build integration

// Run with: go test -tags=integration -v ./test/integration/...
//
// Tests that NewIdempotentHandler delivers each unique event exactly once
// to the inner handler, even when the same message is re-delivered.

package integration

import (
	"context"
	"sync/atomic"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"kafka-user-service/internal/db"
	"kafka-user-service/internal/kafka"
	"kafka-user-service/internal/models"
)

// TestIdempotentHandler_DeduplicatesOnSameEventID proves that:
//   - the inner handler is called exactly once for a given (consumer_group, topic, event_id)
//   - a second delivery with the same triple is silently skipped (no error, no inner call)
//   - processed_events contains exactly one row after both deliveries
func TestIdempotentHandler_DeduplicatesOnSameEventID(t *testing.T) {
	// Use port 15433 to avoid conflict with the IntegrationSuite (port 15432).
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Username("postgres").
		Password("postgres").
		Database("idempotentdb").
		Port(15433),
	)
	require.NoError(t, pg.Start())
	defer func() { _ = pg.Stop() }()

	dsn := "host=localhost port=15433 user=postgres password=postgres dbname=idempotentdb sslmode=disable"
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(gormDB))

	// inner counts how many times it is invoked.
	var callCount int32
	inner := func(_ context.Context, _ kafka.MessageMeta, _ *models.UserEvent) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	handler := kafka.NewIdempotentHandler(gormDB, inner)

	meta := kafka.MessageMeta{
		Topic:         "test.topic",
		Partition:     0,
		Offset:        42,
		ConsumerGroup: "test-group",
	}
	event := &models.UserEvent{
		EventID:   "evt-abc-123",
		EventType: "CREATED",
		UserID:    "user-001",
		Name:      "Alice",
		Email:     "alice@example.com",
		Age:       30,
	}

	ctx := context.Background()

	// First delivery — inner must run.
	require.NoError(t, handler(ctx, meta, event))
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount),
		"inner handler should be called on first delivery")

	// Second delivery (same event_id, same group, same topic) — inner must NOT run.
	require.NoError(t, handler(ctx, meta, event))
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount),
		"inner handler must not be called a second time for the same event_id")

	// Verify exactly one row in processed_events.
	var count int64
	require.NoError(t, gormDB.Model(&models.ProcessedEvent{}).Count(&count).Error)
	assert.Equal(t, int64(1), count,
		"processed_events must contain exactly one row regardless of delivery count")
}

// TestIdempotentHandler_IndependentPerTopic proves that the same event_id
// on two different topics (Avro vs Proto) is NOT deduplicated against each other.
func TestIdempotentHandler_IndependentPerTopic(t *testing.T) {
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Username("postgres").
		Password("postgres").
		Database("idempotentdb2").
		Port(15434),
	)
	require.NoError(t, pg.Start())
	defer func() { _ = pg.Stop() }()

	dsn := "host=localhost port=15434 user=postgres password=postgres dbname=idempotentdb2 sslmode=disable"
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(gormDB))

	var callCount int32
	inner := func(_ context.Context, _ kafka.MessageMeta, _ *models.UserEvent) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	// Both Avro and Proto consumers share the same gormDB (same as production wiring).
	handler := kafka.NewIdempotentHandler(gormDB, inner)

	event := &models.UserEvent{
		EventID:   "shared-event-id",
		EventType: "CREATED",
		UserID:    "user-002",
	}

	avroMeta := kafka.MessageMeta{
		Topic:         "com.br4.user.core.event.avro",
		Partition:     0,
		Offset:        10,
		ConsumerGroup: "user-service-avro",
	}
	protoMeta := kafka.MessageMeta{
		Topic:         "com.br4.user.core.event.proto",
		Partition:     0,
		Offset:        10,
		ConsumerGroup: "user-service-proto",
	}

	ctx := context.Background()

	// Avro consumer handles the event.
	require.NoError(t, handler(ctx, avroMeta, event))
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount), "avro delivery should call inner")

	// Proto consumer handles the same event — different group+topic, so NOT a duplicate.
	require.NoError(t, handler(ctx, protoMeta, event))
	assert.Equal(t, int32(2), atomic.LoadInt32(&callCount),
		"proto delivery must also call inner (different topic+group)")

	// Two rows in processed_events — one per (group, topic, event_id).
	var count int64
	require.NoError(t, gormDB.Model(&models.ProcessedEvent{}).Count(&count).Error)
	assert.Equal(t, int64(2), count,
		"processed_events must have two independent rows for two topics")
}

// TestIdempotentHandler_MissingEventID proves that a message without an event_id
// (pre-migration message) falls through to the inner handler without being recorded.
func TestIdempotentHandler_MissingEventID(t *testing.T) {
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Username("postgres").
		Password("postgres").
		Database("idempotentdb3").
		Port(15435),
	)
	require.NoError(t, pg.Start())
	defer func() { _ = pg.Stop() }()

	dsn := "host=localhost port=15435 user=postgres password=postgres dbname=idempotentdb3 sslmode=disable"
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(gormDB))

	var callCount int32
	inner := func(_ context.Context, _ kafka.MessageMeta, _ *models.UserEvent) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	handler := kafka.NewIdempotentHandler(gormDB, inner)

	meta := kafka.MessageMeta{
		Topic: "test.topic", Partition: 0, Offset: 1, ConsumerGroup: "grp",
	}
	// event_id is empty — simulates a message produced before this feature.
	event := &models.UserEvent{EventID: "", EventType: "CREATED", UserID: "u1"}

	ctx := context.Background()
	require.NoError(t, handler(ctx, meta, event))
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount), "inner must still be called")

	// No row recorded because event_id was empty.
	var count int64
	require.NoError(t, gormDB.Model(&models.ProcessedEvent{}).Count(&count).Error)
	assert.Equal(t, int64(0), count, "processed_events must be empty for missing event_id")
}

// TestIdempotentHandler_InnerErrorRollsBackDedup proves that if inner() returns an
// error, the processed_events row is rolled back (the transaction is aborted).
// On the next delivery the handler must call inner() again — the event is not lost.
func TestIdempotentHandler_InnerErrorRollsBackDedup(t *testing.T) {
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Username("postgres").
		Password("postgres").
		Database("idempotentdb4").
		Port(15436),
	)
	require.NoError(t, pg.Start())
	defer func() { _ = pg.Stop() }()

	dsn := "host=localhost port=15436 user=postgres password=postgres dbname=idempotentdb4 sslmode=disable"
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(gormDB))

	var callCount int32
	failOnce := true
	inner := func(_ context.Context, _ kafka.MessageMeta, _ *models.UserEvent) error {
		atomic.AddInt32(&callCount, 1)
		if failOnce {
			failOnce = false
			return assert.AnError // simulate a transient inner error
		}
		return nil
	}

	handler := kafka.NewIdempotentHandler(gormDB, inner)

	meta := kafka.MessageMeta{
		Topic:         "test.topic",
		Partition:     0,
		Offset:        99,
		ConsumerGroup: "test-group-rollback",
	}
	event := &models.UserEvent{EventID: "evt-rollback-test", EventType: "CREATED", UserID: "u3"}

	ctx := context.Background()

	// First delivery — inner fails; transaction must be rolled back.
	err = handler(ctx, meta, event)
	assert.Error(t, err, "handler must propagate inner error so caller does not commit offset")
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount))

	// No row should exist after rollback.
	var count int64
	require.NoError(t, gormDB.Model(&models.ProcessedEvent{}).Count(&count).Error)
	assert.Equal(t, int64(0), count, "processed_events must be empty after rollback")

	// Second delivery (simulating at-least-once re-delivery after restart).
	// inner succeeds this time; the event must be fully processed.
	require.NoError(t, handler(ctx, meta, event))
	assert.Equal(t, int32(2), atomic.LoadInt32(&callCount),
		"inner must be called again after the first delivery rolled back")

	// Row now persisted.
	require.NoError(t, gormDB.Model(&models.ProcessedEvent{}).Count(&count).Error)
	assert.Equal(t, int64(1), count, "processed_events must have one row after successful delivery")
}

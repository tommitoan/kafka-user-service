package db

import (
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/tommitoan/kafka-user-service/internal/models"
)

// Open connects to Postgres. TranslateError maps driver errors to GORM sentinels
// such as gorm.ErrDuplicatedKey so the repository layer can classify them.
func Open(dsn string, level logger.LogLevel) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.New(log.New(os.Stderr, "", log.LstdFlags), logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  level,
			IgnoreRecordNotFoundError: true, // a missing row is a normal 404, not a DB error
		}),
		TranslateError: true,
	})
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	return db, nil
}

// AutoMigrate runs GORM auto-migration. It is meant for tests with embedded
// Postgres; the service itself applies the SQL files in migrations/.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&models.User{}, &models.ProcessedEvent{})
}

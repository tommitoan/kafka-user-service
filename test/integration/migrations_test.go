//go:build integration

package integration

import (
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/logger"

	"github.com/tommitoan/kafka-user-service/internal/db"
	"github.com/tommitoan/kafka-user-service/internal/models"
	"github.com/tommitoan/kafka-user-service/internal/repository"
)

// TestMigrations_EmailUniqueOnlyAmongLiveUsers applies the real SQL migrations
// (the other tests use AutoMigrate) and checks the schema they produce.
func TestMigrations_EmailUniqueOnlyAmongLiveUsers(t *testing.T) {
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Username("postgres").
		Password("postgres").
		Database("migrationsdb").
		Port(15434),
	)
	require.NoError(t, pg.Start())
	defer func() { _ = pg.Stop() }()

	require.NoError(t, db.RunMigrations(
		"postgres://postgres:postgres@localhost:15434/migrationsdb?sslmode=disable",
		"../../migrations",
	))

	gormDB, err := db.Open("host=localhost port=15434 user=postgres password=postgres dbname=migrationsdb sslmode=disable", logger.Silent)
	require.NoError(t, err)

	repo := repository.NewUserRepository(gormDB)
	ctx := t.Context()

	first := &models.User{Name: "Alice", Email: "alice@example.com"}
	require.NoError(t, repo.Create(ctx, first))

	// A second live user with the same email is rejected...
	err = repo.Create(ctx, &models.User{Name: "Impostor", Email: "alice@example.com"})
	assert.ErrorIs(t, err, repository.ErrEmailTaken)

	// ...but once the first user is soft-deleted the email can be reused.
	require.NoError(t, repo.Delete(ctx, first.ID))
	assert.NoError(t, repo.Create(ctx, &models.User{Name: "Alice again", Email: "alice@example.com"}))

	var live int64
	require.NoError(t, gormDB.Model(&models.User{}).Where("email = ?", "alice@example.com").Count(&live).Error)
	assert.EqualValues(t, 1, live)

}

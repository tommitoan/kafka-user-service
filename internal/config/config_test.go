package config_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"kafka-user-service/internal/config"
)

func TestLoad_Defaults(t *testing.T) {
	// No config file in a temp dir → should use defaults
	cfg, err := config.Load(t.TempDir())
	require.NoError(t, err)

	assert.Equal(t, "0.0.0.0", cfg.Server.Host)
	assert.Equal(t, 8080, cfg.Server.Port)
	assert.Equal(t, "localhost", cfg.Database.Host)
	assert.Equal(t, 5432, cfg.Database.Port)
	assert.Equal(t, "disable", cfg.Database.SSLMode)
	assert.Equal(t, "http://localhost:8081", cfg.Kafka.SchemaRegistry)
	assert.Equal(t, []string{"localhost:9092"}, cfg.Kafka.Brokers)
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("APP_DATABASE_HOST", "db.prod")
	t.Setenv("APP_SERVER_PORT", "9090")

	cfg, err := config.Load(t.TempDir())
	require.NoError(t, err)

	assert.Equal(t, "db.prod", cfg.Database.Host)
	assert.Equal(t, 9090, cfg.Server.Port)
}

func TestLoad_FromFile(t *testing.T) {
	dir := t.TempDir()
	content := `
server:
  host: "127.0.0.1"
  port: 3000
database:
  host: "mydb"
  port: 5433
  user: "admin"
  password: "secret"
  name: "testdb"
  sslmode: "require"
kafka:
  brokers:
    - "kafka1:9092"
    - "kafka2:9092"
  group_id: "my-group"
  schema_registry: "http://sr:8081"
`
	require.NoError(t, os.WriteFile(dir+"/config.yaml", []byte(content), 0644))

	cfg, err := config.Load(dir)
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1", cfg.Server.Host)
	assert.Equal(t, 3000, cfg.Server.Port)
	assert.Equal(t, "mydb", cfg.Database.Host)
	assert.Equal(t, []string{"kafka1:9092", "kafka2:9092"}, cfg.Kafka.Brokers)
}

func TestDatabaseConfig_DSN(t *testing.T) {
	cfg := config.DatabaseConfig{
		Host: "localhost", Port: 5432, User: "u", Password: "p", Name: "db", SSLMode: "disable",
	}
	assert.Equal(t, "host=localhost port=5432 user=u password=p dbname=db sslmode=disable", cfg.DSN())
	assert.Equal(t, "postgres://u:p@localhost:5432/db?sslmode=disable", cfg.MigrateURL())
}

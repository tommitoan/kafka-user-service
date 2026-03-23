package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
}

type ServerConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Name     string `mapstructure:"name"`
	SSLMode  string `mapstructure:"sslmode"`
}

func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode,
	)
}

// MigrateURL returns a URL-style DSN required by golang-migrate.
func (d DatabaseConfig) MigrateURL() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		d.User, d.Password, d.Host, d.Port, d.Name, d.SSLMode,
	)
}

type KafkaConfig struct {
	Brokers        []string      `mapstructure:"brokers"`
	GroupID        string        `mapstructure:"group_id"`
	SchemaRegistry string        `mapstructure:"schema_registry"`
	Topics         []TopicConfig `mapstructure:"topics"`
}

// TopicConfig holds the definition of a single Kafka topic.
// Add new topics here (or in config.yaml) — EnsureTopics will create them automatically.
type TopicConfig struct {
	Name              string `mapstructure:"name"`
	NumPartitions     int    `mapstructure:"num_partitions"`
	ReplicationFactor int    `mapstructure:"replication_factor"`
}

func Load(path string) (*Config, error) {
	v := viper.New()

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(path)
	v.AddConfigPath(".")

	v.SetEnvPrefix("APP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.user", "postgres")
	v.SetDefault("database.password", "postgres")
	v.SetDefault("database.name", "userdb")
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.group_id", "user-service")
	v.SetDefault("kafka.schema_registry", "http://localhost:8081")
	v.SetDefault("kafka.topics", []map[string]interface{}{
		{"name": "com.br4.user.core.event.avro",  "num_partitions": 3, "replication_factor": 1},
		{"name": "com.br4.user.core.event.proto", "num_partitions": 3, "replication_factor": 1},
	})

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &cfg, nil
}

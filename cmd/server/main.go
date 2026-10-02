package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"gorm.io/gorm/logger"

	"github.com/tommitoan/kafka-user-service/docs"
	"github.com/tommitoan/kafka-user-service/internal/api"
	"github.com/tommitoan/kafka-user-service/internal/config"
	"github.com/tommitoan/kafka-user-service/internal/db"
	"github.com/tommitoan/kafka-user-service/internal/kafka"
	"github.com/tommitoan/kafka-user-service/internal/repository"
	"github.com/tommitoan/kafka-user-service/internal/service"
)

const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(".")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ── Database ──────────────────────────────────────────────────────────────
	database, err := db.Open(cfg.Database.DSN(), logger.Warn)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	if err := db.RunMigrations(cfg.Database.MigrateURL(), "./migrations"); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	// ── Kafka ─────────────────────────────────────────────────────────────────
	topics := make([]kafka.TopicDefinition, len(cfg.Kafka.Topics))
	for i, t := range cfg.Kafka.Topics {
		topics[i] = kafka.TopicDefinition{
			Name:              t.Name,
			NumPartitions:     t.NumPartitions,
			ReplicationFactor: t.ReplicationFactor,
		}
	}
	if err := kafka.EnsureTopics(cfg.Kafka.Brokers, topics); err != nil {
		return fmt.Errorf("ensure kafka topics: %w", err)
	}

	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.SchemaRegistry)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	consumer := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.GroupID, cfg.Kafka.SchemaRegistry)
	defer consumer.Close()

	// ── HTTP ──────────────────────────────────────────────────────────────────
	userSvc := service.NewUserService(repository.NewUserRepository(database), producer)

	docs.SwaggerInfo.Host = fmt.Sprintf("localhost:%d", cfg.Server.Port)

	router := gin.Default()
	api.RegisterUI(router)
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	api.NewUserHandler(userSvc).RegisterRoutes(router)

	srv := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Consumers ─────────────────────────────────────────────────────────────
	// Each handler dedupes on (consumer_group, topic, event_id) before logging.
	var wg sync.WaitGroup
	startConsumer := func(name string, start func(context.Context, kafka.EventHandler) error) {
		handler := kafka.NewIdempotentHandler(database, kafka.LoggingHandler(name))
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := start(ctx, handler); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("consumer stopped", "format", name, "error", err)
			}
		}()
	}
	startConsumer("avro", consumer.StartAvro)
	startConsumer("proto", consumer.StartProto)

	// ── Serve until a signal arrives or the listener fails ────────────────────
	serveErr := make(chan error, 1)
	go func() {
		slog.Info("server listening",
			"addr", srv.Addr,
			"ui", fmt.Sprintf("http://localhost:%d/", cfg.Server.Port),
			"swagger", fmt.Sprintf("http://localhost:%d/swagger/index.html", cfg.Server.Port),
		)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		stop()
		wg.Wait()
		return fmt.Errorf("listen: %w", err)
	case <-ctx.Done():
	}

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}
	wg.Wait() // let in-flight handlers finish before the reader and DB are closed
	slog.Info("server exited")
	return nil
}

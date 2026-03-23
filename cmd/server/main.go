package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"kafka-user-service/docs"
	"kafka-user-service/internal/api"
	"kafka-user-service/internal/config"
	"kafka-user-service/internal/db"
	"kafka-user-service/internal/kafka"
	"kafka-user-service/internal/repository"
	"kafka-user-service/internal/service"
)

func main() {
	cfg, err := config.Load(".")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// ── Database ──────────────────────────────────────────────────────────────
	database, err := db.New(db.Config{
		Host:     cfg.Database.Host,
		Port:     cfg.Database.Port,
		User:     cfg.Database.User,
		Password: cfg.Database.Password,
		DBName:   cfg.Database.Name,
		SSLMode:  cfg.Database.SSLMode,
	})
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}

	if err := db.RunMigrations(cfg.Database.MigrateURL(), "./migrations"); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	// ── Kafka topics ────────────────────────────────────────────────────────────
	// Convert config topic definitions → kafka.TopicDefinition
	kafkaTopics := make([]kafka.TopicDefinition, len(cfg.Kafka.Topics))
	for i, t := range cfg.Kafka.Topics {
		kafkaTopics[i] = kafka.TopicDefinition{
			Name:              t.Name,
			NumPartitions:     t.NumPartitions,
			ReplicationFactor: t.ReplicationFactor,
		}
	}
	if err := kafka.EnsureTopics(cfg.Kafka.Brokers, kafkaTopics); err != nil {
		log.Fatalf("ensure kafka topics: %v", err)
	}

	// ── Kafka ─────────────────────────────────────────────────────────────────
	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.SchemaRegistry)
	if err != nil {
		log.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	consumer := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.GroupID, cfg.Kafka.SchemaRegistry)
	defer consumer.Close()

	// ── Wiring ────────────────────────────────────────────────────────────────
	userRepo := repository.NewUserRepository(database)
	userSvc := service.NewUserService(userRepo, producer)
	userHandler := api.NewUserHandler(userSvc)

	// ── Swagger host (reflects actual configured port) ──────────────────────────
	docs.SwaggerInfo.Host = fmt.Sprintf("localhost:%d", cfg.Server.Port)

	// ── HTTP server ───────────────────────────────────────────────────────────
	router := gin.Default()

	// Web UI at /
	api.RegisterUI(router)

	// Swagger at /swagger/index.html
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// REST API
	userHandler.RegisterRoutes(router)

	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler: router,
	}

	// ── Consumers (background) ────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := consumer.StartAvro(ctx, kafka.LoggingHandler("avro")); err != nil {
			log.Printf("[Consumer] Avro stopped: %v", err)
		}
	}()

	go func() {
		if err := consumer.StartProto(ctx, kafka.LoggingHandler("proto")); err != nil {
			log.Printf("[Consumer] Proto stopped: %v", err)
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	go func() {
		log.Printf("Server listening on %s", srv.Addr)
		log.Printf("Web UI:  http://localhost:%d/", cfg.Server.Port)
		log.Printf("Swagger: http://localhost:%d/swagger/index.html", cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("server shutdown: %v", err)
	}

	log.Println("Server exited")
}

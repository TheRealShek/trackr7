//go:build ignore
// +build ignore

package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/TheRealShek/trackr7/admin"
	"github.com/TheRealShek/trackr7/auth"
	"github.com/TheRealShek/trackr7/cache"
	"github.com/TheRealShek/trackr7/db"
	"github.com/TheRealShek/trackr7/ingest"
	"github.com/TheRealShek/trackr7/writer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

type stdLogger struct {
	l *log.Logger
}

func (s stdLogger) Info(msg string, fields ...any) {
	s.l.Printf("INFO: %s %v", msg, fields)
}

func (s stdLogger) Error(msg string, fields ...any) {
	s.l.Printf("ERROR: %s %v", msg, fields)
}

func main() {
	ctx := context.Background()
	logger := stdLogger{l: log.New(os.Stdout, "trackr7 ", log.LstdFlags)}

	dsn := os.Getenv("TRACKR7_DSN")
	if dsn == "" {
		log.Fatal("TRACKR7_DSN is required")
	}

	brokersEnv := os.Getenv("TRACKR7_KAFKA_BROKERS")
	if brokersEnv == "" {
		log.Fatal("TRACKR7_KAFKA_BROKERS is required")
	}
	brokers := strings.Split(brokersEnv, ",")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect pool: %v", err)
	}
	defer pool.Close()

	dbCfg := db.DBConfig{Pool: pool}.WithDefaults()
	if err := dbCfg.Validate(); err != nil {
		log.Fatalf("db config: %v", err)
	}

	keyCache, err := auth.NewKeyCache(dbCfg, 5*time.Minute, rate.Every(time.Second), 10)
	if err != nil {
		log.Fatalf("key cache: %v", err)
	}
	keyCache.Logger = logger
	keyCache.OnCacheHit = func() { logger.Info("auth cache hit") }
	keyCache.OnCacheMiss = func() { logger.Info("auth cache miss") }
	keyCache.OnRateLimit = func(keyID string) { logger.Info("rate limit", "key_id", keyID) }
	keyCache.OnRevoked = func(keyID string) { logger.Info("revoked", "key_id", keyID) }

	kafkaWriter := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: "location.raw",
	}
	defer kafkaWriter.Close()

	ingestHandler, err := ingest.NewHandler(ingest.Config{
		Kafka:          kafkaWriter,
		Auth:           keyCache,
		DB:             dbCfg,
		Logger:         logger,
		OnPingAccepted: func() { logger.Info("ping accepted") },
		OnPingRejected: func(reason string) { logger.Info("ping rejected", "reason", reason) },
		OnKafkaError:   func(err error) { logger.Error("kafka error", "error", err) },
	})
	if err != nil {
		log.Fatalf("ingest handler: %v", err)
	}

	store, err := cache.NewStore(cache.Config{
		KafkaBrokers:     brokers,
		Topic:            "location.raw",
		Namespace:        "example",
		Logger:           logger,
		OnCacheUpdated:   func(entityID string) { logger.Info("cache updated", "entity_id", entityID) },
		OnMessageSkipped: func(reason string) { logger.Info("cache skip", "reason", reason) },
	})
	if err != nil {
		log.Fatalf("cache store: %v", err)
	}

	worker, err := writer.NewWorker(writer.Config{
		KafkaBrokers: brokers,
		Topic:        "location.raw",
		Namespace:    "example",
		DB:           pool,
		DBConfig:     dbCfg,
		Logger:       logger,
		OnBatchFlushed: func(count int, duration time.Duration) {
			logger.Info("batch flushed", "count", count, "duration", duration)
		},
		OnFlushError:     func(err error) { logger.Error("flush error", "error", err) },
		OnMessageSkipped: func(reason string) { logger.Info("writer skip", "reason", reason) },
	})
	if err != nil {
		log.Fatalf("writer: %v", err)
	}

	km, err := admin.NewKeyManager(pool, dbCfg)
	if err != nil {
		log.Fatalf("key manager: %v", err)
	}
	km.SetCache(keyCache)

	go func() {
		if err := keyCache.Run(ctx); err != nil {
			logger.Error("key cache stopped", "error", err)
		}
	}()
	go func() {
		if err := store.Run(ctx); err != nil {
			logger.Error("cache store stopped", "error", err)
		}
	}()
	go func() {
		if err := worker.Run(ctx); err != nil {
			logger.Error("writer stopped", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/ping", ingestHandler)
	mux.HandleFunc("/ready", store.ReadinessHandler())

	srv := &http.Server{Addr: ":8080", Handler: mux}
	log.Printf("listening on %s", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server: %v", err)
	}

	_ = km
}

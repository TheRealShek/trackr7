// Package cache consumes location messages from Kafka and keeps the latest
// location per entity in memory.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TheRealShek/trackr7/schema"
	"github.com/segmentio/kafka-go"
)

const (
	cacheGroupPrefix = "trackr7.cache."
	messageVersion   = 1
)

// Config configures a cache store.
type Config struct {
	// KafkaBrokers lists bootstrap brokers for the reader.
	KafkaBrokers []string
	// KafkaDialer configures TLS/SASL if needed.
	KafkaDialer *kafka.Dialer
	// Topic is the Kafka topic to consume.
	Topic string
	// Namespace scopes the consumer group ID.
	Namespace string
	// Logger records operational messages. Nil-safe.
	Logger schema.Logger
	// Observability hooks — optional, nil-safe.
	// OnCacheUpdated fires when a location is stored.
	OnCacheUpdated func(entityID string)
	// OnMessageSkipped fires when a message is skipped (e.g., unsupported version).
	OnMessageSkipped func(reason string)
}

type entry struct {
	Location  schema.Location
	FetchedAt time.Time
}

type rawLocationMessage struct {
	UUID       string  `json:"uuid"`
	EntityID   string  `json:"entity_id"`
	EntityType string  `json:"entity_type"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	TS         int64   `json:"ts"`
	V          int     `json:"v"`
}

// Store keeps the latest location for each entity in memory.
type Store struct {
	reader *kafka.Reader
	logger schema.Logger

	mu               sync.RWMutex
	entries          map[string]entry
	ready            atomic.Bool
	onCacheUpdated   func(entityID string)
	onMessageSkipped func(reason string)
}

// NewStore creates a new Kafka-backed cache store.
func NewStore(cfg Config) (*Store, error) {
	if strings.TrimSpace(cfg.Namespace) == "" {
		return nil, fmt.Errorf("%w: Namespace is required", schema.ErrNamespaceRequired)
	}
	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("%w: KafkaBrokers is empty", schema.ErrInvalidConfig)
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("%w: Topic is empty", schema.ErrInvalidConfig)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		GroupID:     cacheGroupPrefix + cfg.Namespace,
		GroupTopics: []string{cfg.Topic},
		Dialer:      cfg.KafkaDialer,
	})

	store := &Store{
		reader:           reader,
		logger:           schema.SafeLogger(cfg.Logger),
		entries:          make(map[string]entry),
		onCacheUpdated:   cfg.OnCacheUpdated,
		onMessageSkipped: cfg.OnMessageSkipped,
	}

	return store, nil
}

// Run consumes Kafka messages until ctx is canceled.
func (s *Store) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("%w: store is nil", schema.ErrInvalidConfig)
	}
	defer s.reader.Close()

	for {
		message, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		if err := s.handleMessage(message); err != nil {
			return err
		}

		if err := s.reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit kafka offset: %w", err)
		}
	}
}

// Get returns the latest location for entityID if present.
func (s *Store) Get(entityID string) (schema.Location, time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.entries[entityID]
	if !ok {
		return schema.Location{}, time.Time{}, false
	}
	return entry.Location, entry.FetchedAt, true
}

// ReadinessHandler reports 503 until the first valid location message is stored.
func (s *Store) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Store) handleMessage(message kafka.Message) error {
	var payload rawLocationMessage
	if err := json.Unmarshal(message.Value, &payload); err != nil {
		return fmt.Errorf("decode kafka message: %w", err)
	}

	if payload.V != messageVersion {
		s.logger.Info("skipping unsupported cache message version", "version", payload.V, "uuid", payload.UUID, "entity_id", payload.EntityID)
		s.safeCallOnMessageSkipped("unsupported_version")
		return nil
	}

	now := time.Now().UTC()
	location := schema.Location{
		UUID:       payload.UUID,
		EntityID:   payload.EntityID,
		EntityType: payload.EntityType,
		Lat:        payload.Lat,
		Lng:        payload.Lng,
		TS:         payload.TS,
	}

	s.mu.Lock()
	s.entries[payload.EntityID] = entry{Location: location, FetchedAt: now}
	s.mu.Unlock()

	s.ready.Store(true)
	s.safeCallOnCacheUpdated(payload.EntityID)
	return nil
}

// safe hook callers that recover from panics and log via store logger
func (s *Store) safeCallOnCacheUpdated(entityID string) {
	if s.onCacheUpdated == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("OnCacheUpdated hook panicked", "panic", r)
		}
	}()
	s.onCacheUpdated(entityID)
}

func (s *Store) safeCallOnMessageSkipped(reason string) {
	if s.onMessageSkipped == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("OnMessageSkipped hook panicked", "panic", r)
		}
	}()
	s.onMessageSkipped(reason)
}

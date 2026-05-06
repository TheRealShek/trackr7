// Package ingest provides the HTTP handler for location ping ingestion.
//
// The handler authenticates requests via auth.Middleware, validates the JSON
// payload, stamps a server UTC timestamp, and produces a message to Kafka.
// It does not access the database directly — authentication is delegated
// to the auth package.
//
// All error responses are JSON: {"error": "message"}.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/TheRealShek/trackr7/auth"
	"github.com/TheRealShek/trackr7/db"
	"github.com/TheRealShek/trackr7/schema"
	"github.com/segmentio/kafka-go"
)

// Producer is the interface for producing messages to Kafka.
// *kafka.Writer satisfies this interface. Exported so callers can
// provide custom implementations or test fakes.
type Producer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Config configures the ingest handler.
type Config struct {
	// Kafka is the producer used to write location messages.
	// Required. *kafka.Writer satisfies the Producer interface.
	Kafka Producer

	// Auth is the key cache used for Bearer token authentication.
	// Required.
	Auth *auth.KeyCache

	// DB holds database configuration. Included for consistency with
	// other trackr7 packages — ingest does not query the database.
	DB db.DBConfig

	// MaxBodyKB is the maximum request body size in kilobytes.
	// Default: 1.
	MaxBodyKB int

	// Logger for operational messages. Nil-safe — nil disables logging.
	Logger schema.Logger
	// Observability hooks — optional, nil-safe.
	// OnPingAccepted fires after a ping is accepted (202).
	OnPingAccepted func()
	// OnPingRejected fires when a ping is rejected (e.g., invalid_json).
	OnPingRejected func(reason string)
	// OnKafkaError fires when producing to Kafka fails.
	OnKafkaError func(err error)
}

// NewHandler creates an HTTP handler for location ping ingestion.
//
// The returned handler enforces POST-only, limits request body size,
// authenticates via auth.Middleware, validates the payload, stamps
// server UTC time, and produces to Kafka. Returns 202 on success.
//
// Returns ErrInvalidConfig if required Config fields are missing.
func NewHandler(cfg Config) (http.Handler, error) {
	if cfg.Kafka == nil {
		return nil, fmt.Errorf("%w: Kafka producer is nil", schema.ErrInvalidConfig)
	}
	if cfg.Auth == nil {
		return nil, fmt.Errorf("%w: Auth is nil", schema.ErrInvalidConfig)
	}
	if cfg.MaxBodyKB <= 0 {
		cfg.MaxBodyKB = 1
	}

	log := schema.SafeLogger(cfg.Logger)
	maxBytes := int64(cfg.MaxBodyKB) * 1024

	inner := &handler{
		producer:       cfg.Kafka,
		logger:         log,
		maxBodyBytes:   maxBytes,
		onPingAccepted: cfg.OnPingAccepted,
		onPingRejected: cfg.OnPingRejected,
		onKafkaError:   cfg.OnKafkaError,
	}

	// Auth middleware wraps the inner handler — runs before decode.
	authed := cfg.Auth.Middleware(inner)

	// Outer handler enforces method + body size before auth.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		authed.ServeHTTP(w, r)
	}), nil
}

// handler is the inner HTTP handler that decodes, validates, and produces.
type handler struct {
	producer       Producer
	logger         schema.Logger
	maxBodyBytes   int64
	onPingAccepted func()
	onPingRejected func(reason string)
	onKafkaError   func(err error)
}

// pingRequest is the expected JSON body from clients.
// entity_type is deliberately absent — it comes from auth context.
type pingRequest struct {
	UUID     string  `json:"uuid"`
	EntityID string  `json:"entity_id"`
	Lat      float64 `json:"lat"`
	Lng      float64 `json:"lng"`
}

// kafkaMsg matches the HLD Kafka message shape exactly.
type kafkaMsg struct {
	UUID       string  `json:"uuid"`
	EntityID   string  `json:"entity_id"`
	EntityType string  `json:"entity_type"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	TS         int64   `json:"ts"`
	V          int     `json:"v"`
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	maxBodyBytes := h.maxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = 1024
	}

	var req pingRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) || (r.ContentLength > 0 && r.ContentLength > maxBodyBytes) {
			h.safeCallOnPingRejected("body_too_large")
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		h.safeCallOnPingRejected("invalid_json")
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	if err := validatePing(req); err != nil {
		// map human error messages to machine-friendly keys
		reason := "invalid"
		switch {
		case strings.Contains(err.Error(), "uuid"):
			reason = "missing_uuid"
		case strings.Contains(err.Error(), "entity_id"):
			reason = "missing_entity_id"
		case strings.Contains(err.Error(), "lat") || strings.Contains(err.Error(), "lng"):
			reason = "invalid_lat_lng"
		}
		h.safeCallOnPingRejected(reason)
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	entityType := auth.EntityTypeFromCtx(r.Context())

	msg := kafkaMsg{
		UUID:       req.UUID,
		EntityID:   req.EntityID,
		EntityType: entityType,
		Lat:        req.Lat,
		Lng:        req.Lng,
		TS:         time.Now().UTC().UnixMilli(),
		V:          1,
	}

	value, err := json.Marshal(msg)
	if err != nil {
		h.logger.Error("failed to marshal kafka message", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if err := h.producer.WriteMessages(r.Context(), kafka.Message{
		Key:   []byte(req.EntityID),
		Value: value,
	}); err != nil {
		h.logger.Error("kafka produce failed", "error", err, "entity_id", req.EntityID)
		h.safeCallOnKafkaError(err)
		writeJSONError(w, http.StatusInternalServerError, "failed to produce message")
		return
	}

	h.safeCallOnPingAccepted()
	w.WriteHeader(http.StatusAccepted)
}

// safe hook callers that recover from panics and log via handler logger
func (h *handler) safeCallOnPingAccepted() {
	if h.onPingAccepted == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("OnPingAccepted hook panicked", "panic", r)
		}
	}()
	h.onPingAccepted()
}

func (h *handler) safeCallOnPingRejected(reason string) {
	if h.onPingRejected == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("OnPingRejected hook panicked", "panic", r)
		}
	}()
	h.onPingRejected(reason)
}

func (h *handler) safeCallOnKafkaError(err error) {
	if h.onKafkaError == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("OnKafkaError hook panicked", "panic", r)
		}
	}()
	h.onKafkaError(err)
}

// validatePing checks that all required fields are present and coordinates
// are within valid ranges.
func validatePing(p pingRequest) error {
	if p.UUID == "" {
		return errors.New("uuid is required")
	}
	if p.EntityID == "" {
		return errors.New("entity_id is required")
	}
	if p.Lat < -90 || p.Lat > 90 {
		return errors.New("lat must be between -90 and 90")
	}
	if p.Lng < -180 || p.Lng > 180 {
		return errors.New("lng must be between -180 and 180")
	}
	return nil
}

// writeJSONError writes a JSON error response with the given status code.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

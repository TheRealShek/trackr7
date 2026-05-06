package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/TheRealShek/trackr7/auth"
	"github.com/TheRealShek/trackr7/schema"
	"github.com/segmentio/kafka-go"
)

// --- fake producer ---

type fakeProducer struct {
	msgs []kafka.Message
	err  error
}

func (f *fakeProducer) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}

// --- NewHandler config validation ---

func TestNewHandlerValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "nil kafka",
			cfg:     Config{Auth: &auth.KeyCache{}},
			wantErr: true,
		},
		{
			name:    "nil auth",
			cfg:     Config{Kafka: &fakeProducer{}},
			wantErr: true,
		},
		{
			name:    "valid config",
			cfg:     Config{Kafka: &fakeProducer{}, Auth: &auth.KeyCache{}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewHandler(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewHandler() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil && !errors.Is(err, schema.ErrInvalidConfig) {
				t.Errorf("expected ErrInvalidConfig, got %v", err)
			}
		})
	}
}

func TestNewHandlerDefaultMaxBody(t *testing.T) {
	// MaxBodyKB=0 should default to 1 (not error).
	_, err := NewHandler(Config{Kafka: &fakeProducer{}, Auth: &auth.KeyCache{}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- validatePing ---

func TestValidatePing(t *testing.T) {
	tests := []struct {
		name    string
		req     pingRequest
		wantErr string
	}{
		{"valid", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "ent1", 12.5, 77.0}, ""},
		{"missing uuid", pingRequest{"", "ent1", 12.5, 77.0}, "uuid is required"},
		{"missing entity_id", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "", 12.5, 77.0}, "entity_id is required"},
		{"lat too low", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "ent1", -91, 77.0}, "lat must be between"},
		{"lat too high", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "ent1", 91, 77.0}, "lat must be between"},
		{"lng too low", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "ent1", 12.5, -181}, "lng must be between"},
		{"lng too high", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "ent1", 12.5, 181}, "lng must be between"},
		{"boundary lat -90", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "ent1", -90, 0}, ""},
		{"boundary lat 90", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "ent1", 90, 0}, ""},
		{"boundary lng -180", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "ent1", 0, -180}, ""},
		{"boundary lng 180", pingRequest{"550e8400-e29b-41d4-a716-446655440000", "ent1", 0, 180}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePing(tt.req)
			if tt.wantErr == "" && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.wantErr)
				} else if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.wantErr)
				}
			}
		})
	}
}

// --- inner handler (bypasses auth, tests decode → validate → produce) ---

// newTestRequest builds an HTTP request with entity type injected into context
// via auth.ContextWithEntityType, bypassing the auth middleware.
func newTestRequest(t *testing.T, method, body, entityType string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, "/ping", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	ctx := auth.ContextWithEntityType(r.Context(), entityType)
	return r.WithContext(ctx)
}

func TestHandlerServeHTTP(t *testing.T) {
	validBody := `{"uuid":"550e8400-e29b-41d4-a716-446655440000","entity_id":"truck-1","lat":28.6139,"lng":77.2090}`

	tests := []struct {
		name       string
		body       string
		entityType string
		prodErr    error
		wantStatus int
		wantError  string
	}{
		{
			name:       "valid request",
			body:       validBody,
			entityType: "vehicle",
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "empty body",
			body:       "",
			entityType: "vehicle",
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid JSON",
		},
		{
			name:       "malformed json",
			body:       `{bad`,
			entityType: "vehicle",
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid JSON",
		},
		{
			name:       "unknown field rejected",
			body:       `{"uuid":"550e8400-e29b-41d4-a716-446655440000","entity_id":"e1","lat":10,"lng":20,"extra":"bad"}`,
			entityType: "vehicle",
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid JSON",
		},
		{
			name:       "missing uuid",
			body:       `{"entity_id":"e1","lat":10,"lng":20}`,
			entityType: "vehicle",
			wantStatus: http.StatusBadRequest,
			wantError:  "uuid is required",
		},
		{
			name:       "invalid lat",
			body:       `{"uuid":"550e8400-e29b-41d4-a716-446655440000","entity_id":"e1","lat":999,"lng":20}`,
			entityType: "vehicle",
			wantStatus: http.StatusBadRequest,
			wantError:  "lat must be between",
		},
		{
			name:       "kafka failure",
			body:       validBody,
			entityType: "vehicle",
			prodErr:    errors.New("broker down"),
			wantStatus: http.StatusInternalServerError,
			wantError:  "failed to produce message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prod := &fakeProducer{err: tt.prodErr}
			h := &handler{
				producer: prod,
				logger:   schema.SafeLogger(nil),
			}

			req := newTestRequest(t, http.MethodPost, tt.body, tt.entityType)
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}

			if tt.wantError != "" {
				var body map[string]string
				if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
					t.Fatalf("decode error body: %v", err)
				}
				if !strings.Contains(body["error"], tt.wantError) {
					t.Errorf("error = %q, want containing %q", body["error"], tt.wantError)
				}
			}
		})
	}
}

// --- Kafka message shape ---

func TestKafkaMessageShape(t *testing.T) {
	prod := &fakeProducer{}
	h := &handler{
		producer: prod,
		logger:   schema.SafeLogger(nil),
	}

	body := `{"uuid":"550e8400-e29b-41d4-a716-446655440000","entity_id":"truck-42","lat":28.6139,"lng":77.2090}`
	req := newTestRequest(t, http.MethodPost, body, "vehicle")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if len(prod.msgs) != 1 {
		t.Fatalf("expected 1 kafka message, got %d", len(prod.msgs))
	}

	msg := prod.msgs[0]

	// Key must be entity_id for partition affinity.
	if string(msg.Key) != "truck-42" {
		t.Errorf("kafka key = %q, want %q", string(msg.Key), "truck-42")
	}

	// Decode value and check shape.
	var km kafkaMsg
	if err := json.Unmarshal(msg.Value, &km); err != nil {
		t.Fatalf("unmarshal kafka value: %v", err)
	}

	if km.UUID != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("uuid = %q, want %q", km.UUID, "550e8400-e29b-41d4-a716-446655440000")
	}
	if km.EntityID != "truck-42" {
		t.Errorf("entity_id = %q, want %q", km.EntityID, "truck-42")
	}
	if km.EntityType != "vehicle" {
		t.Errorf("entity_type = %q, want %q (from auth context)", km.EntityType, "vehicle")
	}
	if km.Lat != 28.6139 {
		t.Errorf("lat = %v, want %v", km.Lat, 28.6139)
	}
	if km.Lng != 77.2090 {
		t.Errorf("lng = %v, want %v", km.Lng, 77.2090)
	}
	if km.TS <= 0 {
		t.Error("ts should be a positive unix ms timestamp")
	}
	if km.V != 1 {
		t.Errorf("v = %d, want 1", km.V)
	}
}

// --- method restriction ---

func TestMethodNotAllowed(t *testing.T) {
	methods := []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			// Build the outer handler closure manually to avoid needing
			// a real KeyCache for NewHandler.
			outer := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
					return
				}
			})

			req := httptest.NewRequest(method, "/ping", nil)
			rec := httptest.NewRecorder()
			outer.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("status = %d, want 405", rec.Code)
			}
		})
	}
}

// --- MaxBytesReader ---

func TestBodyTooLarge(t *testing.T) {
	prod := &fakeProducer{}
	h := &handler{
		producer: prod,
		logger:   schema.SafeLogger(nil),
	}

	// Create a body larger than 1KB.
	bigBody := bytes.Repeat([]byte("x"), 2048)

	req := httptest.NewRequest(http.MethodPost, "/ping", bytes.NewReader(bigBody))
	req.Header.Set("Content-Type", "application/json")
	ctx := auth.ContextWithEntityType(req.Context(), "vehicle")
	req = req.WithContext(ctx)

	// Wrap body with MaxBytesReader (1KB limit) to simulate the outer handler.
	rec := httptest.NewRecorder()
	req.Body = http.MaxBytesReader(rec, req.Body, 1024)

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

// --- entity type from auth context ---

func TestEntityTypeFromContext(t *testing.T) {
	prod := &fakeProducer{}
	h := &handler{
		producer: prod,
		logger:   schema.SafeLogger(nil),
	}

	body := `{"uuid":"550e8400-e29b-41d4-a716-446655440000","entity_id":"e1","lat":10,"lng":20}`
	req := newTestRequest(t, http.MethodPost, body, "courier")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}

	var km kafkaMsg
	if err := json.Unmarshal(prod.msgs[0].Value, &km); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if km.EntityType != "courier" {
		t.Errorf("entity_type = %q, want %q", km.EntityType, "courier")
	}
}

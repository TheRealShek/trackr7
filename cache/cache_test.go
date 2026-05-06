package cache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TheRealShek/trackr7/schema"
	"github.com/segmentio/kafka-go"
)

func TestNewStoreValidation(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantErr   bool
		wantError string
	}{
		{
			name:      "empty namespace",
			cfg:       Config{KafkaBrokers: []string{"localhost:9092"}, Topic: "location.raw"},
			wantErr:   true,
			wantError: schema.ErrNamespaceRequired.Error(),
		},
		{
			name:      "empty brokers",
			cfg:       Config{Topic: "location.raw", Namespace: "ns"},
			wantErr:   true,
			wantError: "KafkaBrokers is empty",
		},
		{
			name:      "empty topic",
			cfg:       Config{KafkaBrokers: []string{"localhost:9092"}, Namespace: "ns"},
			wantErr:   true,
			wantError: "Topic is empty",
		},
		{
			name:    "valid config",
			cfg:     Config{KafkaBrokers: []string{"localhost:9092"}, Topic: "location.raw", Namespace: "ns"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := NewStore(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewStore() error = nil, want error")
				}
				if tt.wantError != "" && !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tt.wantError)
				}
				return
			}

			if err != nil {
				t.Fatalf("NewStore() error = %v", err)
			}
			if store == nil {
				t.Fatal("NewStore() returned nil store")
			}
		})
	}
}

func TestHandleMessageAndGet(t *testing.T) {
	store := &Store{
		logger:  schema.SafeLogger(nil),
		entries: make(map[string]entry),
	}

	first := `{"uuid":"u1","entity_id":"car-1","entity_type":"vehicle","lat":12.34,"lng":56.78,"ts":1700000000000,"v":1}`
	if err := store.handleMessage(kafka.Message{Value: []byte(first)}); err != nil {
		t.Fatalf("handleMessage(): %v", err)
	}

	loc, fetchedAt, ok := store.Get("car-1")
	if !ok {
		t.Fatal("Get() = false, want true")
	}
	if loc.UUID != "u1" || loc.EntityID != "car-1" || loc.EntityType != "vehicle" {
		t.Fatalf("unexpected location: %+v", loc)
	}
	if loc.TS != 1700000000000 {
		t.Fatalf("TS = %d, want 1700000000000", loc.TS)
	}
	if fetchedAt.IsZero() {
		t.Fatal("FetchedAt is zero")
	}
	if time.Since(fetchedAt) > time.Second {
		t.Fatalf("FetchedAt too old: %v", fetchedAt)
	}
	if !store.ready.Load() {
		t.Fatal("ready = false, want true after first valid message")
	}
}

func TestHandleMessageSkipsUnsupportedVersion(t *testing.T) {
	store := &Store{
		logger:  schema.SafeLogger(nil),
		entries: make(map[string]entry),
	}

	msg := `{"uuid":"u2","entity_id":"car-2","entity_type":"vehicle","lat":1,"lng":2,"ts":1700000000001,"v":2}`
	if err := store.handleMessage(kafka.Message{Value: []byte(msg)}); err != nil {
		t.Fatalf("handleMessage(): %v", err)
	}

	_, _, ok := store.Get("car-2")
	if ok {
		t.Fatal("Get() = true, want false for skipped version")
	}
	if store.ready.Load() {
		t.Fatal("ready = true, want false when only unsupported versions were seen")
	}
}

func TestReadinessHandler(t *testing.T) {
	store := &Store{logger: schema.SafeLogger(nil), entries: make(map[string]entry)}
	handler := store.ReadinessHandler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}

	store.ready.Store(true)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestHandleMessageInvalidJSON(t *testing.T) {
	store := &Store{logger: schema.SafeLogger(nil), entries: make(map[string]entry)}
	if err := store.handleMessage(kafka.Message{Value: []byte(`{"uuid":`)}); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestGetMissing(t *testing.T) {
	store := &Store{logger: schema.SafeLogger(nil), entries: make(map[string]entry)}
	loc, fetchedAt, ok := store.Get("missing")
	if ok {
		t.Fatal("Get() = true, want false")
	}
	if loc != (schema.Location{}) {
		t.Fatalf("location = %+v, want zero value", loc)
	}
	if !fetchedAt.IsZero() {
		t.Fatalf("FetchedAt = %v, want zero value", fetchedAt)
	}
}

func TestMessageDecodingShape(t *testing.T) {
	var msg rawLocationMessage
	input := `{"uuid":"u3","entity_id":"car-3","entity_type":"truck","lat":9.1,"lng":8.2,"ts":1700000000002,"v":1}`
	if err := json.Unmarshal([]byte(input), &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg.V != 1 || msg.EntityID != "car-3" || msg.EntityType != "truck" {
		t.Fatalf("decoded message mismatch: %+v", msg)
	}
}

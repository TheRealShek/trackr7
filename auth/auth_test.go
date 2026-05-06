package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// --- extractBearer ---

func TestExtractBearer(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
		wantOK bool
	}{
		{"valid", "Bearer abc123", "abc123", true},
		{"case insensitive", "bearer abc123", "abc123", true},
		{"mixed case", "BEARER abc123", "abc123", true},
		{"with extra spaces in token", "Bearer   abc123  ", "abc123", true},
		{"empty header", "", "", false},
		{"no bearer prefix", "Basic abc123", "", false},
		{"bearer only no token", "Bearer ", "", false},
		{"bearer only no space", "Bearer", "", false},
		{"too short", "Bear", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.header != "" {
				r.Header.Set("Authorization", tt.header)
			}
			got, ok := extractBearer(r)
			if ok != tt.wantOK {
				t.Errorf("ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("token = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- EntityTypeFromCtx ---

func TestEntityTypeFromCtx(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{"present", context.WithValue(context.Background(), entityTypeKey, "driver"), "driver"},
		{"missing", context.Background(), ""},
		{"wrong type", context.WithValue(context.Background(), entityTypeKey, 42), ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EntityTypeFromCtx(tt.ctx)
			if got != tt.want {
				t.Errorf("EntityTypeFromCtx() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- Evict ---

func TestEvict(t *testing.T) {
	hash := sha256.Sum256([]byte("test-token"))

	kc := &KeyCache{
		byID:   make(map[string]*keyMetadata),
		byHash: make(map[[32]byte]*keyMetadata),
	}

	meta := &keyMetadata{
		KeyID:      "key-1",
		Hash:       hash,
		EntityType: "driver",
		Limiter:    rate.NewLimiter(10, 20),
	}
	kc.byID["key-1"] = meta
	kc.byHash[hash] = meta

	// Evict existing key.
	kc.Evict("key-1")

	if _, ok := kc.byID["key-1"]; ok {
		t.Error("byID still contains evicted key")
	}
	if _, ok := kc.byHash[hash]; ok {
		t.Error("byHash still contains evicted key")
	}

	// Evict non-existent key — should be a no-op, no panic.
	kc.Evict("does-not-exist")
}

// --- Middleware (cache-only, no DB) ---

// okHandler is a downstream handler that records that it was called and
// captures the entity type from context.
type okHandler struct {
	called     bool
	entityType string
}

func (h *okHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.called = true
	h.entityType = EntityTypeFromCtx(r.Context())
	w.WriteHeader(http.StatusOK)
}

// newTestCache returns a KeyCache with pre-populated entries, no DB needed.
func newTestCache(entries ...*keyMetadata) *KeyCache {
	kc := &KeyCache{
		rateLimit: 100,
		rateBurst: 100,
		byID:      make(map[string]*keyMetadata),
		byHash:    make(map[[32]byte]*keyMetadata),
	}
	for _, m := range entries {
		kc.byID[m.KeyID] = m
		kc.byHash[m.Hash] = m
	}
	return kc
}

func TestMiddleware(t *testing.T) {
	validToken := "my-secret-key"
	validHash := sha256.Sum256([]byte(validToken))

	revokedToken := "revoked-key"
	revokedHash := sha256.Sum256([]byte(revokedToken))

	rateLimitedToken := "limited-key"
	rateLimitedHash := sha256.Sum256([]byte(rateLimitedToken))

	entries := []*keyMetadata{
		{
			KeyID:      "key-valid",
			Hash:       validHash,
			EntityType: "driver",
			Limiter:    rate.NewLimiter(rate.Inf, 0), // unlimited for test
		},
		{
			KeyID:      "key-revoked",
			Hash:       revokedHash,
			EntityType: "driver",
			Limiter:    rate.NewLimiter(rate.Inf, 0),
			Revoked:    true,
		},
		{
			KeyID:      "key-limited",
			Hash:       rateLimitedHash,
			EntityType: "courier",
			Limiter:    rate.NewLimiter(0, 0), // zero rate = always reject
		},
	}

	tests := []struct {
		name           string
		header         string
		wantStatus     int
		wantCalled     bool
		wantEntityType string
		wantError      string
	}{
		{
			name:       "no auth header",
			header:     "",
			wantStatus: http.StatusUnauthorized,
			wantError:  "missing or malformed Authorization header",
		},
		{
			name:       "malformed auth header",
			header:     "Basic abc123",
			wantStatus: http.StatusUnauthorized,
			wantError:  "missing or malformed Authorization header",
		},
		{
			name:       "unknown token (cache miss, no DB)",
			header:     "Bearer unknown-token",
			wantStatus: http.StatusUnauthorized,
			wantError:  "invalid API key",
		},
		{
			name:       "revoked key",
			header:     "Bearer " + revokedToken,
			wantStatus: http.StatusUnauthorized,
			wantError:  "API key revoked",
		},
		{
			name:       "rate limited key",
			header:     "Bearer " + rateLimitedToken,
			wantStatus: http.StatusTooManyRequests,
			wantError:  "rate limit exceeded",
		},
		{
			name:           "valid key",
			header:         "Bearer " + validToken,
			wantStatus:     http.StatusOK,
			wantCalled:     true,
			wantEntityType: "driver",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kc := newTestCache(entries...)

			// For "unknown token" test, the cache-miss path calls loadFromDB
			// which needs a Pool. We nil it out — the DB call will panic.
			// Instead, we override lookup by not setting a pool. The pgx
			// QueryRow on nil pool will panic, so we need to handle this.
			// For unit tests we only test the cache-hit paths. The "unknown
			// token" case is handled by verifying the middleware responds 401
			// when no entry matches. We need the cache to have no DB fallback.
			//
			// To make this work without a DB, we'll skip the unknown token
			// test in the unit suite and cover it in integration tests.
			if tt.name == "unknown token (cache miss, no DB)" {
				t.Skip("requires DB for cache-miss fallback; covered by integration tests")
			}

			next := &okHandler{}
			handler := kc.Middleware(next)

			req := httptest.NewRequest(http.MethodPost, "/ping", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}

			if next.called != tt.wantCalled {
				t.Errorf("next.called = %v, want %v", next.called, tt.wantCalled)
			}

			if tt.wantCalled && next.entityType != tt.wantEntityType {
				t.Errorf("entityType = %q, want %q", next.entityType, tt.wantEntityType)
			}

			if tt.wantError != "" {
				var body map[string]string
				if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
					t.Fatalf("failed to decode error response: %v", err)
				}
				if body["error"] != tt.wantError {
					t.Errorf("error = %q, want %q", body["error"], tt.wantError)
				}
				ct := rec.Header().Get("Content-Type")
				if ct != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", ct)
				}
			}
		})
	}
}

// --- writeJSONError ---

func TestWriteJSONError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSONError(rec, http.StatusForbidden, "forbidden")

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"] != "forbidden" {
		t.Errorf("error = %q, want %q", body["error"], "forbidden")
	}
}

// --- Concurrent Evict safety ---

func TestEvictConcurrent(t *testing.T) {
	kc := &KeyCache{
		byID:   make(map[string]*keyMetadata),
		byHash: make(map[[32]byte]*keyMetadata),
	}

	// Populate cache with 100 keys.
	for i := 0; i < 100; i++ {
		token := hex.EncodeToString([]byte{byte(i)})
		hash := sha256.Sum256([]byte(token))
		meta := &keyMetadata{
			KeyID:   token,
			Hash:    hash,
			Limiter: rate.NewLimiter(10, 10),
		}
		kc.byID[token] = meta
		kc.byHash[hash] = meta
	}

	// Concurrently evict all keys — must not race or panic.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			token := hex.EncodeToString([]byte{byte(idx)})
			kc.Evict(token)
		}(i)
	}
	wg.Wait()

	if len(kc.byID) != 0 {
		t.Errorf("byID has %d entries, want 0", len(kc.byID))
	}
	if len(kc.byHash) != 0 {
		t.Errorf("byHash has %d entries, want 0", len(kc.byHash))
	}
}

// --- Middleware injects entity type correctly ---

func TestMiddlewareContextPropagation(t *testing.T) {
	token := "context-test-token"
	hash := sha256.Sum256([]byte(token))

	kc := newTestCache(&keyMetadata{
		KeyID:      "ctx-key",
		Hash:       hash,
		EntityType: "warehouse",
		Limiter:    rate.NewLimiter(rate.Inf, 0),
	})

	var captured string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = EntityTypeFromCtx(r.Context())
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	kc.Middleware(next).ServeHTTP(rec, req)

	if captured != "warehouse" {
		t.Errorf("context entity type = %q, want %q", captured, "warehouse")
	}
}

// --- Rate limiter enforcement ---

func TestMiddlewareRateLimiting(t *testing.T) {
	token := "rate-test-token"
	hash := sha256.Sum256([]byte(token))

	// Allow exactly 1 request, then reject.
	kc := newTestCache(&keyMetadata{
		KeyID:      "rate-key",
		Hash:       hash,
		EntityType: "driver",
		Limiter:    rate.NewLimiter(rate.Every(time.Hour), 1),
	})

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := kc.Middleware(next)

	// First request should pass.
	req1 := httptest.NewRequest(http.MethodPost, "/", nil)
	req1.Header.Set("Authorization", "Bearer "+token)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("first request: status = %d, want 200", rec1.Code)
	}

	// Second request should be rate-limited.
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("Authorization", "Bearer "+token)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("second request: status = %d, want 429", rec2.Code)
	}
}

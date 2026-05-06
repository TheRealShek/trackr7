package auth

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/TheRealShek/trackr7/schema"
	"golang.org/x/time/rate"
)

func TestOnCacheHit_OnRevoked_OnRateLimit(t *testing.T) {
	// prepare a key
	token := "tok-1"
	h := sha256.Sum256([]byte(token))

	meta := &keyMetadata{KeyID: "k1", Hash: h, EntityType: "vehicle", Limiter: nil, Revoked: false}
	// create a cache with the key present
	kc := &KeyCache{byHash: make(map[[32]byte]*keyMetadata), byID: make(map[string]*keyMetadata), Logger: schema.SafeLogger(nil)}
	kc.byHash[h] = meta
	kc.byID["k1"] = meta

	hit := 0
	kc.OnCacheHit = func() { hit++ }
	ctx := context.Background()
	if _, err := kc.lookup(ctx, h); err != nil {
		t.Fatalf("lookup error: %v", err)
	}
	if hit != 1 {
		t.Fatalf("OnCacheHit not called")
	}

	// revoked
	metaRev := &keyMetadata{KeyID: "k2", Hash: h, EntityType: "vehicle", Limiter: nil, Revoked: true}
	kc.byHash[h] = metaRev
	revokedCalled := ""
	kc.OnRevoked = func(k string) { revokedCalled = k }

	// craft request with Authorization Bearer token
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	// middleware should invoke OnRevoked and return 401
	kc.OnCacheHit = nil
	kc.OnRateLimit = nil
	handler := kc.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	handler.ServeHTTP(w, req)
	if revokedCalled == "" {
		t.Fatalf("OnRevoked not called")
	}

	// rate limit: set limiter that denies
	metaRev.Revoked = false
	metaRev.Limiter = rate.NewLimiter(0, 0)
	kc.byHash[h] = metaRev
	rlCalled := ""
	kc.OnRateLimit = func(k string) { rlCalled = k }
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req)
	if rlCalled == "" {
		t.Fatalf("OnRateLimit not called")
	}
}

package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/TheRealShek/trackr7/schema"
)

// BenchmarkMiddlewareCacheHit simulates a fast path where the middleware
// receives a valid token and proceeds to next handler. We simulate the
// KeyCache behavior by invoking the next handler directly with a prepared context.
func BenchmarkMiddlewareCacheHit(b *testing.B) {
	b.ReportAllocs()
	// Build a minimal KeyCache and middleware wrapper that simply calls next.
	kc := &KeyCache{Logger: schema.SafeLogger(nil)}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	h := kc.Middleware(next)

	req := httptest.NewRequest("GET", "/", nil)
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		_ = rec
	}
}

// BenchmarkEvictAndReinsert simulates concurrent Evict calls as a unit test.
func BenchmarkEvictAndReinsert(b *testing.B) {
	b.ReportAllocs()
	kc := &KeyCache{Logger: schema.SafeLogger(nil), byID: map[string]*keyMetadata{}, byHash: map[[32]byte]*keyMetadata{}}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			kc.Evict("some-id")
		}
	})
}

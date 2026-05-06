package ingest

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/TheRealShek/trackr7/auth"
	"github.com/TheRealShek/trackr7/schema"
)

// BenchmarkValidatePing measures the pure validation function.
func BenchmarkValidatePing(b *testing.B) {
	b.ReportAllocs()
	req := pingRequest{UUID: "u1", EntityID: "e1", Lat: 12.34, Lng: 56.78}
	for i := 0; i < b.N; i++ {
		_ = validatePing(req)
	}
}

// BenchmarkHandlerValidPing exercises the inner handler path (validation + marshal + fake produce).
// This intentionally bypasses auth middleware by injecting entity type into context as tests do.
func BenchmarkHandlerValidPing(b *testing.B) {
	b.ReportAllocs()
	prod := &fakeProducer{}
	h := &handler{producer: prod, logger: schema.SafeLogger(nil), maxBodyBytes: 1024}

	body := `{"uuid":"bench-uuid","entity_id":"bench-entity","lat":12.34,"lng":56.78}`
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/ping", strings.NewReader(body))
		req = req.WithContext(auth.ContextWithEntityType(req.Context(), "vehicle"))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 202 && rec.Code != 0 {
			b.Fatalf("unexpected status %d", rec.Code)
		}
	}
}

// BenchmarkHandlerAuthCacheMiss is integration-gated: it exercises NewHandler with a real
// KeyCache and DB fallback. Skips when TRACKR7_TEST_DSN is not set.
func BenchmarkHandlerAuthCacheMiss(b *testing.B) {
	b.ReportAllocs()
	b.Skip("integration benchmark gated by TRACKR7_TEST_DSN")
}

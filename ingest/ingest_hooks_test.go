package ingest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/TheRealShek/trackr7/schema"
	"github.com/segmentio/kafka-go"
)

type dummyProducer struct {
	err  error
	last kafka.Message
}

func (d *dummyProducer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if len(msgs) > 0 {
		d.last = msgs[0]
	}
	return d.err
}

func TestHooks_OnPingAccepted_OnPingRejected_OnKafkaError(t *testing.T) {
	// OnPingAccepted
	prod := &dummyProducer{}
	h := &handler{
		producer:     prod,
		logger:       schema.SafeLogger(nil),
		maxBodyBytes: 1024,
	}

	called := false
	h.onPingAccepted = func() { called = true }

	body := `{"uuid":"550e8400-e29b-41d4-a716-446655440000","entity_id":"e1","lat":1,"lng":2}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	// set ContentLength so the handler sees it
	req.ContentLength = int64(len(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)
	if !called {
		t.Fatalf("OnPingAccepted not called")
	}

	// OnPingRejected invalid_json
	calledReason := ""
	h.onPingRejected = func(r string) { calledReason = r }
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{"))
	req2.ContentLength = 1
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)
	if calledReason != "invalid_json" {
		t.Fatalf("OnPingRejected invalid_json expected, got %q", calledReason)
	}

	// OnPingRejected body_too_large
	calledReason = ""
	h.maxBodyBytes = 1
	large := strings.Repeat("a", 10)
	req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(large))
	req3.ContentLength = int64(len(large))
	w3 := httptest.NewRecorder()
	h.ServeHTTP(w3, req3)
	if calledReason != "body_too_large" {
		t.Fatalf("OnPingRejected body_too_large expected, got %q", calledReason)
	}

	// OnKafkaError
	prod.err = fmt.Errorf("kafka down")
	calledErr := error(nil)
	h.onKafkaError = func(e error) { calledErr = e }
	h.maxBodyBytes = 1024
	req4 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req4.ContentLength = int64(len(body))
	w4 := httptest.NewRecorder()
	h.ServeHTTP(w4, req4)
	if calledErr == nil || calledErr.Error() != "kafka down" {
		t.Fatalf("OnKafkaError not called with expected error: %v", calledErr)
	}
}

package cache

import (
	"testing"
	"time"

	"github.com/TheRealShek/trackr7/schema"
	"github.com/segmentio/kafka-go"
)

func TestCacheHooks_OnCacheUpdated_OnMessageSkipped(t *testing.T) {
	var updated string
	var skipped string
	s := &Store{logger: schema.SafeLogger(nil), entries: make(map[string]entry), onCacheUpdated: func(id string) { updated = id }, onMessageSkipped: func(r string) { skipped = r }}

	// valid message
	payload := `{"uuid":"u1","entity_id":"e1","entity_type":"car","lat":1,"lng":2,"ts":123,"v":1}`
	if err := s.handleMessage(kafka.Message{Value: []byte(payload)}); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}
	if updated != "e1" {
		t.Fatalf("OnCacheUpdated not called, got %q", updated)
	}

	// unsupported version
	payload2 := `{"uuid":"u2","entity_id":"e2","entity_type":"car","lat":1,"lng":2,"ts":123,"v":2}`
	if err := s.handleMessage(kafka.Message{Value: []byte(payload2)}); err != nil {
		t.Fatalf("handleMessage error: %v", err)
	}
	if skipped != "unsupported_version" {
		t.Fatalf("OnMessageSkipped not called, got %q", skipped)
	}

	// ensure readiness set
	_, _, ok := s.Get("e1")
	if !ok {
		t.Fatalf("expected entry for e1")
	}

	// small time gap to ensure fetchedAt exists
	time.Sleep(1 * time.Millisecond)
}

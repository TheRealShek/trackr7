package writer

import (
	"testing"

	"github.com/TheRealShek/trackr7/schema"
	"github.com/segmentio/kafka-go"
)

func TestOnMessageSkipped(t *testing.T) {
	var received string
	w := &Worker{logger: schema.SafeLogger(nil), onMessageSkipped: func(r string) { received = r }}

	payload := `{"uuid":"u1","entity_id":"e1","entity_type":"car","lat":1,"lng":2,"ts":3,"v":2}`
	item, err := w.classifyMessage(kafka.Message{Value: []byte(payload)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if item.write {
		t.Fatalf("expected item.write=false for unsupported version")
	}
	if received != "unsupported_version" {
		t.Fatalf("OnMessageSkipped not invoked, got %q", received)
	}
}

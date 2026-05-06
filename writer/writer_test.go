package writer

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/TheRealShek/trackr7/db"
	"github.com/TheRealShek/trackr7/schema"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func TestNewWorkerValidation(t *testing.T) {
	dummyPool := &pgxpool.Pool{}
	validDBConfig := db.DBConfig{Pool: dummyPool}.WithDefaults()

	tests := []struct {
		name      string
		cfg       Config
		wantErr   bool
		wantError string
	}{
		{
			name:      "nil db",
			cfg:       Config{KafkaBrokers: []string{"localhost:9092"}, Topic: "location.raw", Namespace: "ns", DB: nil, DBConfig: validDBConfig},
			wantErr:   true,
			wantError: "DB pool is nil",
		},
		{
			name:      "empty namespace",
			cfg:       Config{KafkaBrokers: []string{"localhost:9092"}, Topic: "location.raw", Namespace: " ", DB: dummyPool, DBConfig: validDBConfig},
			wantErr:   true,
			wantError: schema.ErrNamespaceRequired.Error(),
		},
		{
			name:      "empty brokers",
			cfg:       Config{Topic: "location.raw", Namespace: "ns", DB: dummyPool, DBConfig: validDBConfig},
			wantErr:   true,
			wantError: "KafkaBrokers is empty",
		},
		{
			name:      "empty topic",
			cfg:       Config{KafkaBrokers: []string{"localhost:9092"}, Namespace: "ns", DB: dummyPool, DBConfig: validDBConfig},
			wantErr:   true,
			wantError: "Topic is empty",
		},
		{
			name: "defaults applied",
			cfg: Config{
				KafkaBrokers: []string{"localhost:9092"},
				Topic:        "location.raw",
				Namespace:    "ns",
				DB:           dummyPool,
				DBConfig:     validDBConfig,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker, err := NewWorker(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewWorker() error = nil, want error")
				}
				if tt.wantError != "" && !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tt.wantError)
				}
				return
			}

			if err != nil {
				t.Fatalf("NewWorker() error = %v", err)
			}
			if worker.batchSize != defaultBatchSize {
				t.Fatalf("batchSize = %d, want %d", worker.batchSize, defaultBatchSize)
			}
			if worker.flushEvery != defaultFlushEvery {
				t.Fatalf("flushEvery = %v, want %v", worker.flushEvery, defaultFlushEvery)
			}
		})
	}
}

func TestClassifyMessage(t *testing.T) {
	worker := &Worker{logger: schema.SafeLogger(nil)}

	tests := []struct {
		name      string
		payload   string
		wantWrite bool
		wantErr   bool
	}{
		{
			name:      "version 1 writes",
			payload:   `{"uuid":"u1","entity_id":"e1","entity_type":"car","lat":1,"lng":2,"ts":3,"v":1}`,
			wantWrite: true,
		},
		{
			name:      "unsupported version skips",
			payload:   `{"uuid":"u1","entity_id":"e1","entity_type":"car","lat":1,"lng":2,"ts":3,"v":2}`,
			wantWrite: false,
		},
		{
			name:    "invalid json fails",
			payload: `{"uuid":`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item, err := worker.classifyMessage(kafka.Message{Value: []byte(tt.payload)})
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("classifyMessage() error = %v", err)
			}
			if item.write != tt.wantWrite {
				t.Fatalf("write = %v, want %v", item.write, tt.wantWrite)
			}
		})
	}
}

func TestBuildBatchPlan(t *testing.T) {
	items := []batchItem{
		{message: kafka.Message{Key: []byte("1")}, row: rawLocationMessage{UUID: "u1"}, write: true},
		{message: kafka.Message{Key: []byte("2")}, row: rawLocationMessage{UUID: "u2"}, write: false},
	}

	plan := buildBatchPlan(items)
	if len(plan.commitMessages) != 2 {
		t.Fatalf("commitMessages = %d, want 2", len(plan.commitMessages))
	}
	if len(plan.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(plan.rows))
	}
	if plan.rows[0].UUID != "u1" {
		t.Fatalf("rows[0].UUID = %q, want %q", plan.rows[0].UUID, "u1")
	}
}

func TestBuildInsertSQL(t *testing.T) {
	cfg := db.DBConfig{
		LocationsTable: "trackr.locations",
		LocationColumns: db.LocationColumnMap{
			UUID:       "id",
			EntityID:   "device_id",
			EntityType: "device_type",
			Lat:        "latitude",
			Lng:        "longitude",
			TS:         "recorded_at",
		},
	}.WithDefaults()

	sql := buildInsertSQL(cfg)
	for _, want := range []string{"trackr.locations", "id", "device_id", "device_type", "latitude", "longitude", "recorded_at", "ON CONFLICT (id) DO NOTHING"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestWriteBatchIntegration(t *testing.T) {
	dsn := os.Getenv("TRACKR7_TEST_DSN")
	if dsn == "" {
		t.Skip("TRACKR7_TEST_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	dbCfg := db.DBConfig{Pool: pool}.WithDefaults()
	if err := dbCfg.Validate(); err != nil {
		t.Fatalf("db config: %v", err)
	}

	if !tableExists(ctx, pool, dbCfg.LocationsTable) {
		t.Skipf("table %q not present", dbCfg.LocationsTable)
	}

	worker := &Worker{db: pool, dbConfig: dbCfg, insertSQL: buildInsertSQL(dbCfg), logger: schema.SafeLogger(nil)}
	location := rawLocationMessage{
		UUID:       fmt.Sprintf("writer-test-%d", time.Now().UnixNano()),
		EntityID:   "entity-1",
		EntityType: "vehicle",
		Lat:        12.34,
		Lng:        56.78,
		TS:         time.Now().UTC().UnixMilli(),
		V:          1,
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM "+dbCfg.LocationsTable+" WHERE "+dbCfg.LocationColumns.UUID+" = $1", location.UUID)
	}()

	if err := worker.writeBatch(ctx, []rawLocationMessage{location, location}); err != nil {
		t.Fatalf("writeBatch: %v", err)
	}

	var count int
	row := pool.QueryRow(ctx, "SELECT count(*) FROM "+dbCfg.LocationsTable+" WHERE "+dbCfg.LocationColumns.UUID+" = $1", location.UUID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("scan count: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func tableExists(ctx context.Context, pool *pgxpool.Pool, table string) bool {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); err != nil {
		return false
	}
	return exists
}

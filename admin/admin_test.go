package admin

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/TheRealShek/trackr7/db"
	"github.com/TheRealShek/trackr7/schema"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeEvictor struct {
	called bool
	keyID  string
	panic  bool
}

func (f *fakeEvictor) Evict(keyID string) {
	f.called = true
	f.keyID = keyID
	if f.panic {
		panic("evict failed")
	}
}

func TestNewKeyManagerValidation(t *testing.T) {
	dummyPool := &pgxpool.Pool{}
	validCfg := db.DBConfig{Pool: dummyPool}.WithDefaults()

	tests := []struct {
		name      string
		pool      *pgxpool.Pool
		cfg       db.DBConfig
		wantErr   bool
		wantError string
	}{
		{
			name:      "nil pool",
			pool:      nil,
			cfg:       validCfg,
			wantErr:   true,
			wantError: schema.ErrInvalidConfig.Error(),
		},
		{
			name:    "invalid db config",
			pool:    dummyPool,
			cfg:     db.DBConfig{Pool: dummyPool, APIKeysTable: ""},
			wantErr: false,
		},
		{
			name:    "valid config",
			pool:    dummyPool,
			cfg:     validCfg,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km, err := NewKeyManager(tt.pool, tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewKeyManager() error = nil, want error")
				}
				if tt.wantError != "" && !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewKeyManager() error = %v", err)
			}
			if km == nil {
				t.Fatal("NewKeyManager() returned nil manager")
			}
		})
	}
}

func TestGeneratePlaintextKey(t *testing.T) {
	for i := 0; i < 3; i++ {
		key, err := generatePlaintextKey()
		if err != nil {
			t.Fatalf("generatePlaintextKey(): %v", err)
		}
		decoded, err := base64.RawURLEncoding.DecodeString(key)
		if err != nil {
			t.Fatalf("decode plaintext key: %v", err)
		}
		if len(decoded) != 32 {
			t.Fatalf("decoded length = %d, want 32", len(decoded))
		}
	}
}

func TestGenerateUUID(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	for i := 0; i < 5; i++ {
		id, err := generateUUID()
		if err != nil {
			t.Fatalf("generateUUID(): %v", err)
		}
		if !re.MatchString(id) {
			t.Fatalf("uuid = %q, want v4 uuid format", id)
		}
	}
}

func TestSQLBuilders(t *testing.T) {
	cfg := db.DBConfig{
		LocationsTable: "trackr.locations",
		APIKeysTable:   "trackr.api_keys",
		AuditLogTable:  "trackr.audit_log",
		APIKeyColumns: db.APIKeyColumnMap{
			KeyID:      "kid",
			KeyHash:    "khash",
			EntityType: "etype",
			Revoked:    "revoked",
			CreatedAt:  "created_at",
		},
		AuditLogColumns: db.AuditLogColumnMap{
			KeyID:  "kid",
			Action: "action",
			TS:     "ts",
		},
	}.WithDefaults()
	km := &KeyManager{cfg: cfg}

	tests := []struct {
		name string
		got  string
		want []string
	}{
		{"insert key SQL", km.insertKeySQL(), []string{"trackr.api_keys", "kid", "khash", "etype", "created_at"}},
		{"revoke key SQL", km.revokeKeySQL(), []string{"trackr.api_keys", "revoked", "kid"}},
		{"list keys SQL", km.listKeysSQL(), []string{"trackr.api_keys", "ORDER BY", "created_at", "kid"}},
		{"insert audit SQL", km.insertAuditSQL(), []string{"trackr.audit_log", "kid", "action", "ts"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, want := range tt.want {
				if !strings.Contains(tt.got, want) {
					t.Fatalf("SQL %q does not contain %q", tt.got, want)
				}
			}
		})
	}
}

func TestSetCacheAndEvictRecovery(t *testing.T) {
	km := &KeyManager{}
	fake := &fakeEvictor{panic: true}
	km.SetCache(fake)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("evictCache panicked: %v", r)
		}
	}()

	km.evictCache("key-1")
	if !fake.called || fake.keyID != "key-1" {
		t.Fatalf("evictor call = %+v, want key-1", fake)
	}
}

func TestListKeysEmptyManagerPanicsHandledByIntegrationOnly(t *testing.T) {
	t.Skip("integration test covers ListKeys against a real database")
}

func TestKeyLifecycleIntegration(t *testing.T) {
	dsn := os.Getenv("TRACKR7_TEST_DSN")
	if dsn == "" {
		t.Skip("TRACKR7_TEST_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	if !tableExists(ctx, pool, dbCfg.APIKeysTable) {
		t.Skipf("table %q not present", dbCfg.APIKeysTable)
	}
	if !tableExists(ctx, pool, dbCfg.AuditLogTable) {
		t.Skipf("table %q not present", dbCfg.AuditLogTable)
	}

	km, err := NewKeyManager(pool, dbCfg)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}

	fake := &fakeEvictor{}
	km.SetCache(fake)

	entityType := fmt.Sprintf("admin-test-%d", time.Now().UnixNano())
	plaintext, err := km.CreateKey(ctx, entityType)
	if err != nil {
		t.Fatalf("CreateKey: %v", err)
	}
	hash := sha256.Sum256([]byte(plaintext))
	hashHex := hex.EncodeToString(hash[:])

	var keyID string
	var revoked bool
	var createdAt int64
	row := pool.QueryRow(ctx,
		"SELECT "+dbCfg.APIKeyColumns.KeyID+", "+dbCfg.APIKeyColumns.Revoked+", "+dbCfg.APIKeyColumns.CreatedAt+" FROM "+dbCfg.APIKeysTable+" WHERE "+dbCfg.APIKeyColumns.KeyHash+" = $1",
		hashHex,
	)
	if err := row.Scan(&keyID, &revoked, &createdAt); err != nil {
		t.Fatalf("scan api key: %v", err)
	}
	if keyID == "" {
		t.Fatal("keyID is empty")
	}
	if revoked {
		t.Fatal("revoked = true, want false after create")
	}
	if createdAt <= 0 {
		t.Fatalf("createdAt = %d, want positive", createdAt)
	}

	keys, err := km.ListKeys(ctx)
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	var found bool
	for _, key := range keys {
		if key.KeyID == keyID {
			found = true
			if key.EntityType != entityType {
				t.Fatalf("entity_type = %q, want %q", key.EntityType, entityType)
			}
			if key.Revoked {
				t.Fatal("Revoked = true, want false after create")
			}
			if key.CreatedAt != createdAt {
				t.Fatalf("CreatedAt = %d, want %d", key.CreatedAt, createdAt)
			}
		}
	}
	if !found {
		t.Fatal("created key not returned by ListKeys")
	}

	if err := km.RevokeKey(ctx, keyID); err != nil {
		t.Fatalf("RevokeKey: %v", err)
	}
	if !fake.called || fake.keyID != keyID {
		t.Fatalf("cache eviction not called correctly: %+v", fake)
	}

	row = pool.QueryRow(ctx,
		"SELECT "+dbCfg.APIKeyColumns.Revoked+" FROM "+dbCfg.APIKeysTable+" WHERE "+dbCfg.APIKeyColumns.KeyID+" = $1",
		keyID,
	)
	if err := row.Scan(&revoked); err != nil {
		t.Fatalf("scan revoked: %v", err)
	}
	if !revoked {
		t.Fatal("revoked = false, want true after revoke")
	}

	var auditCount int
	row = pool.QueryRow(ctx,
		"SELECT count(*) FROM "+dbCfg.AuditLogTable+" WHERE "+dbCfg.AuditLogColumns.KeyID+" = $1",
		keyID,
	)
	if err := row.Scan(&auditCount); err != nil {
		t.Fatalf("scan audit count: %v", err)
	}
	if auditCount < 2 {
		t.Fatalf("auditCount = %d, want at least 2", auditCount)
	}

	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM "+dbCfg.AuditLogTable+" WHERE "+dbCfg.AuditLogColumns.KeyID+" = $1", keyID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM "+dbCfg.APIKeysTable+" WHERE "+dbCfg.APIKeyColumns.KeyID+" = $1", keyID)
	}()
}

func tableExists(ctx context.Context, pool *pgxpool.Pool, table string) bool {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); err != nil {
		return false
	}
	return exists
}

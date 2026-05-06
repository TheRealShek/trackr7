// Package admin manages API key lifecycle for trackr7.
package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/TheRealShek/trackr7/db"
	"github.com/TheRealShek/trackr7/schema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// KeyEvictor is the minimal cache eviction contract used by KeyManager.
type KeyEvictor interface {
	Evict(keyID string)
}

// KeyManager creates, revokes, and lists API keys.
type KeyManager struct {
	pool *pgxpool.Pool
	cfg  db.DBConfig
	log  schema.Logger

	mu       sync.RWMutex
	cache    KeyEvictor
	cacheSet bool
}

// NewKeyManager creates a KeyManager backed by the supplied pool and DB config.
func NewKeyManager(pool *pgxpool.Pool, cfg db.DBConfig) (*KeyManager, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: Pool is nil", schema.ErrInvalidConfig)
	}

	cfg = cfg.WithDefaults()
	cfg.Pool = pool
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &KeyManager{
		pool: pool,
		cfg:  cfg,
		log:  schema.SafeLogger(nil),
	}, nil
}

// SetCache wires an optional cache evictor after construction.
func (km *KeyManager) SetCache(evictor KeyEvictor) {
	km.mu.Lock()
	defer km.mu.Unlock()
	km.cache = evictor
	km.cacheSet = true
}

// CreateKey creates a new API key and records the creation audit event.
func (km *KeyManager) CreateKey(ctx context.Context, entityType string) (string, error) {
	if strings.TrimSpace(entityType) == "" {
		return "", fmt.Errorf("%w: entityType is empty", schema.ErrInvalidConfig)
	}

	plaintext, err := generatePlaintextKey()
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(plaintext))
	keyID, err := generateUUID()
	if err != nil {
		return "", err
	}
	createdAt := time.Now().UTC().UnixMilli()
	keyHash := hex.EncodeToString(hash[:])

	tx, err := km.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if ct, err := tx.Exec(ctx, km.insertKeySQL(), keyID, keyHash, entityType, createdAt); err != nil {
		return "", fmt.Errorf("insert api key: %w", err)
	} else if ct.RowsAffected() == 0 {
		return "", fmt.Errorf("insert api key: no rows affected")
	}
	if ct, err := tx.Exec(ctx, km.insertAuditSQL(), keyID, "created", createdAt); err != nil {
		return "", fmt.Errorf("insert audit log: %w", err)
	} else if ct.RowsAffected() == 0 {
		return "", fmt.Errorf("insert audit log: no rows affected")
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit transaction: %w", err)
	}

	return plaintext, nil
}

// RevokeKey revokes a key and records the revocation audit event.
func (km *KeyManager) RevokeKey(ctx context.Context, keyID string) error {
	if strings.TrimSpace(keyID) == "" {
		return fmt.Errorf("%w: keyID is empty", schema.ErrInvalidConfig)
	}

	now := time.Now().UTC().UnixMilli()
	tx, err := km.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if ct, err := tx.Exec(ctx, km.revokeKeySQL(), keyID); err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	} else if ct.RowsAffected() == 0 {
		return fmt.Errorf("revoke api key: key not found")
	}
	if ct, err := tx.Exec(ctx, km.insertAuditSQL(), keyID, "revoked", now); err != nil {
		return fmt.Errorf("insert audit log: %w", err)
	} else if ct.RowsAffected() == 0 {
		return fmt.Errorf("insert audit log: no rows affected")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	km.evictCache(keyID)
	return nil
}

// ListKeys returns all keys without exposing hashes.
func (km *KeyManager) ListKeys(ctx context.Context) ([]schema.KeyInfo, error) {
	rows, err := km.pool.Query(ctx, km.listKeysSQL())
	if err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	defer rows.Close()

	keys := make([]schema.KeyInfo, 0)
	for rows.Next() {
		var key schema.KeyInfo
		if err := rows.Scan(&key.KeyID, &key.EntityType, &key.Revoked, &key.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan api key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate api keys: %w", err)
	}

	return keys, nil
}

func (km *KeyManager) evictCache(keyID string) {
	km.mu.RLock()
	evictor := km.cache
	km.mu.RUnlock()
	if evictor == nil {
		return
	}

	defer func() {
		_ = recover()
	}()
	evictor.Evict(keyID)
}

func (km *KeyManager) insertKeySQL() string {
	cols := km.cfg.APIKeyColumns
	return fmt.Sprintf(
		"INSERT INTO %s (%s, %s, %s, %s) VALUES ($1, $2, $3, $4)",
		km.cfg.APIKeysTable,
		cols.KeyID,
		cols.KeyHash,
		cols.EntityType,
		cols.CreatedAt,
	)
}

func (km *KeyManager) revokeKeySQL() string {
	cols := km.cfg.APIKeyColumns
	return fmt.Sprintf(
		"UPDATE %s SET %s = true WHERE %s = $1",
		km.cfg.APIKeysTable,
		cols.Revoked,
		cols.KeyID,
	)
}

func (km *KeyManager) listKeysSQL() string {
	cols := km.cfg.APIKeyColumns
	return fmt.Sprintf(
		"SELECT %s, %s, %s, %s FROM %s ORDER BY %s DESC, %s DESC",
		cols.KeyID,
		cols.EntityType,
		cols.Revoked,
		cols.CreatedAt,
		km.cfg.APIKeysTable,
		cols.CreatedAt,
		cols.KeyID,
	)
}

func (km *KeyManager) insertAuditSQL() string {
	cols := km.cfg.AuditLogColumns
	return fmt.Sprintf(
		"INSERT INTO %s (%s, %s, %s) VALUES ($1, $2, $3)",
		km.cfg.AuditLogTable,
		cols.KeyID,
		cols.Action,
		cols.TS,
	)
}

func generatePlaintextKey() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate key: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func generateUUID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate uuid: %w", err)
	}

	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80

	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(buf[0:4]),
		hex.EncodeToString(buf[4:6]),
		hex.EncodeToString(buf[6:8]),
		hex.EncodeToString(buf[8:10]),
		hex.EncodeToString(buf[10:16]),
	), nil
}

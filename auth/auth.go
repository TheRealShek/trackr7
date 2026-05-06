// Package auth provides API key authentication and per-key rate limiting for trackr7.
//
// KeyCache maintains an in-memory cache of API key metadata, backed by periodic
// database refreshes. Middleware extracts Bearer tokens, verifies them against
// cached SHA-256 hashes, enforces revocation, and applies per-key rate limiting.
//
// All database queries use configurable table and column names from db.DBConfig.
// No identifiers are hardcoded.
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/TheRealShek/trackr7/db"
	"github.com/TheRealShek/trackr7/schema"
	"golang.org/x/time/rate"
)

// contextKey is an unexported type for context keys, preventing collisions
// with keys defined in other packages.
type contextKey int

const entityTypeKey contextKey = 0

// Config controls auth behavior that is not part of DB schema mapping.
type Config struct {
	// DBTimeout bounds auth DB calls when the caller context has no deadline.
	// 0 = no library-enforced timeout.
	DBTimeout time.Duration
}

// EntityTypeFromCtx extracts the entity type that Middleware injected into
// the request context. Returns an empty string if not present.
func EntityTypeFromCtx(ctx context.Context) string {
	v, _ := ctx.Value(entityTypeKey).(string)
	return v
}

// ContextWithEntityType returns a copy of ctx with the entity type set.
// Useful for testing handlers that depend on the entity type injected
// by Middleware without requiring a full auth setup.
func ContextWithEntityType(ctx context.Context, entityType string) context.Context {
	return context.WithValue(ctx, entityTypeKey, entityType)
}

// keyMetadata holds cached auth state for a single API key.
type keyMetadata struct {
	KeyID      string
	Hash       [32]byte
	EntityType string
	Limiter    *rate.Limiter
	Revoked    bool
}

// KeyCache provides in-memory API key authentication with per-key rate limiting.
//
// On cache miss, it falls back to a database lookup and populates the cache.
// A background goroutine (Run) periodically refreshes the full cache from the
// database, preserving existing rate.Limiter state for surviving keys.
type KeyCache struct {
	cfg          db.DBConfig
	refreshEvery time.Duration
	rateLimit    rate.Limit
	rateBurst    int
	dbTimeout    time.Duration

	mu     sync.RWMutex
	byID   map[string]*keyMetadata
	byHash map[[32]byte]*keyMetadata
	// Logger receives hook panic reports. Nil-safe.
	Logger schema.Logger

	// Observability hooks — optional, nil-safe.
	// OnCacheHit fires when a cache hit occurs.
	OnCacheHit func()
	// OnCacheMiss fires when a cache miss triggers DB fallback.
	OnCacheMiss func()
	// OnRateLimit fires when a key exceeds its rate limit.
	OnRateLimit func(keyID string)
	// OnRevoked fires when a revoked key is presented.
	OnRevoked func(keyID string)
}

// NewKeyCache creates a KeyCache backed by the database described in cfg.
//
// cfg is passed through WithDefaults() and Validate() internally — callers
// do not need to call those beforehand. refreshEvery controls the background
// refresh interval. rateLimit and rateBurst configure the uniform per-key
// token-bucket rate limiter applied to all keys.
//
// Returns ErrInvalidConfig if the DBConfig is invalid.
func NewKeyCache(cfg db.DBConfig, refreshEvery time.Duration, rateLimit rate.Limit, rateBurst int, options ...Config) (*KeyCache, error) {
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var authCfg Config
	if len(options) > 0 {
		authCfg = options[0]
	}

	return &KeyCache{
		cfg:          cfg,
		refreshEvery: refreshEvery,
		rateLimit:    rateLimit,
		rateBurst:    rateBurst,
		dbTimeout:    authCfg.DBTimeout,
		byID:         make(map[string]*keyMetadata),
		byHash:       make(map[[32]byte]*keyMetadata),
	}, nil
}

// Run starts the background refresh loop. It performs an initial full load
// from the database, then refreshes every refreshEvery interval.
//
// Run blocks until ctx is cancelled and returns ctx.Err() on clean shutdown.
// If the initial load fails, Run returns the error immediately.
// Subsequent refresh failures are silently ignored — stale cache is preferred
// over no cache.
func (kc *KeyCache) Run(ctx context.Context) error {
	if err := kc.refresh(ctx); err != nil {
		return fmt.Errorf("auth: initial key refresh failed: %w", err)
	}

	ticker := time.NewTicker(kc.refreshEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = kc.refresh(ctx)
		}
	}
}

// Evict removes a key from the cache by key ID. This is a cache-only
// operation — it does not update the database. The admin package handles
// DB-side revocation and should call Evict afterward.
//
// If keyID is not in the cache, Evict is a no-op.
func (kc *KeyCache) Evict(keyID string) {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	if meta, ok := kc.byID[keyID]; ok {
		delete(kc.byHash, meta.Hash)
		delete(kc.byID, keyID)
	}
}

// Middleware returns an http.Handler that authenticates requests via Bearer token.
//
// Flow:
//  1. Extract "Authorization: Bearer <token>" header.
//  2. SHA-256 hash the token.
//  3. Look up hash in cache; on miss, fall back to database.
//  4. Reject if key is revoked (401).
//  5. Reject if rate limit exceeded (429).
//  6. Inject entity type into request context; call next handler.
//
// Error responses are JSON: {"error": "message"}.
func (kc *KeyCache) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := extractBearer(r)
		if !ok {
			writeJSONError(w, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}

		hash := sha256.Sum256([]byte(token))

		meta, err := kc.lookup(r.Context(), hash)
		if err != nil {
			writeJSONError(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		if meta.Revoked {
			kc.safeCallOnRevoked(meta.KeyID)
			writeJSONError(w, http.StatusUnauthorized, "API key revoked")
			return
		}

		if !meta.Limiter.Allow() {
			kc.safeCallOnRateLimit(meta.KeyID)
			writeJSONError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}

		ctx := context.WithValue(r.Context(), entityTypeKey, meta.EntityType)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// lookup finds key metadata by hash. Checks cache first (RLock), falls back
// to a database query on miss.
// Timing note: auth is keyed by full SHA-256 hash map lookup.
// No stored-vs-computed byte comparison exists, so no timing oracle.
func (kc *KeyCache) lookup(ctx context.Context, hash [32]byte) (*keyMetadata, error) {
	kc.mu.RLock()
	meta, ok := kc.byHash[hash]
	kc.mu.RUnlock()
	if ok {
		kc.safeCallOnCacheHit()
		return meta, nil
	}

	kc.safeCallOnCacheMiss()
	return kc.loadFromDB(ctx, hash)
}

// loadFromDB queries the database for a key matching the given hash,
// populates the cache on success, and returns the metadata.
// Returns ErrUnauthorized wrapping the DB error if no matching key exists.
func (kc *KeyCache) loadFromDB(ctx context.Context, hash [32]byte) (*keyMetadata, error) {
	cols := kc.cfg.APIKeyColumns
	table := kc.cfg.APIKeysTable
	hashHex := hex.EncodeToString(hash[:])

	query := fmt.Sprintf(
		"SELECT %s, %s, %s FROM %s WHERE %s = $1",
		cols.KeyID, cols.EntityType, cols.Revoked,
		table,
		cols.KeyHash,
	)

	callCtx := ctx
	if kc.dbTimeout > 0 {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, kc.dbTimeout)
			defer cancel()
		}
	}

	row := kc.cfg.Pool.QueryRow(callCtx, query, hashHex)

	var (
		keyID      string
		entityType string
		revoked    bool
	)
	if err := row.Scan(&keyID, &entityType, &revoked); err != nil {
		return nil, fmt.Errorf("%w: %v", schema.ErrUnauthorized, err)
	}

	meta := &keyMetadata{
		KeyID:      keyID,
		Hash:       hash,
		EntityType: entityType,
		Limiter:    rate.NewLimiter(kc.rateLimit, kc.rateBurst),
		Revoked:    revoked,
	}

	kc.mu.Lock()
	kc.byID[keyID] = meta
	kc.byHash[hash] = meta
	kc.mu.Unlock()

	return meta, nil
}

// refresh loads all keys from the database and atomically replaces the cache.
// Existing rate.Limiter instances are preserved for keys that survive the
// refresh, maintaining their token bucket state across reloads.
func (kc *KeyCache) refresh(ctx context.Context) error {
	cols := kc.cfg.APIKeyColumns
	table := kc.cfg.APIKeysTable

	query := fmt.Sprintf(
		"SELECT %s, %s, %s, %s FROM %s",
		cols.KeyID, cols.KeyHash, cols.EntityType, cols.Revoked,
		table,
	)

	callCtx := ctx
	if kc.dbTimeout > 0 {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, kc.dbTimeout)
			defer cancel()
		}
	}

	rows, err := kc.cfg.Pool.Query(callCtx, query)
	if err != nil {
		return fmt.Errorf("auth: refresh query failed: %w", err)
	}
	defer rows.Close()

	// Snapshot existing limiters under read lock before iterating rows.
	kc.mu.RLock()
	oldByID := kc.byID
	kc.mu.RUnlock()

	newByID := make(map[string]*keyMetadata)
	newByHash := make(map[[32]byte]*keyMetadata)

	for rows.Next() {
		var (
			keyID      string
			hashHex    string
			entityType string
			revoked    bool
		)
		if err := rows.Scan(&keyID, &hashHex, &entityType, &revoked); err != nil {
			return fmt.Errorf("auth: refresh scan failed: %w", err)
		}

		decoded, err := hex.DecodeString(hashHex)
		if err != nil || len(decoded) != 32 {
			// Skip entries with corrupt hashes rather than failing the
			// entire refresh. The key simply won't be authenticable.
			continue
		}

		var h [32]byte
		copy(h[:], decoded)

		// Preserve existing rate limiter if the key was already cached,
		// so token bucket state carries across refreshes.
		var limiter *rate.Limiter
		if old, ok := oldByID[keyID]; ok {
			limiter = old.Limiter
		} else {
			limiter = rate.NewLimiter(kc.rateLimit, kc.rateBurst)
		}

		meta := &keyMetadata{
			KeyID:      keyID,
			Hash:       h,
			EntityType: entityType,
			Limiter:    limiter,
			Revoked:    revoked,
		}
		newByID[keyID] = meta
		newByHash[h] = meta
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("auth: refresh iteration failed: %w", err)
	}

	kc.mu.Lock()
	kc.byID = newByID
	kc.byHash = newByHash
	kc.mu.Unlock()

	return nil
}

// safe hook callers that recover from panics and log via KeyCache logger
func (kc *KeyCache) safeCallOnCacheHit() {
	if kc.OnCacheHit == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			schema.SafeLogger(kc.Logger).Error("OnCacheHit hook panicked", "panic", r)
		}
	}()
	kc.OnCacheHit()
}

func (kc *KeyCache) safeCallOnCacheMiss() {
	if kc.OnCacheMiss == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			schema.SafeLogger(kc.Logger).Error("OnCacheMiss hook panicked", "panic", r)
		}
	}()
	kc.OnCacheMiss()
}

func (kc *KeyCache) safeCallOnRateLimit(keyID string) {
	if kc.OnRateLimit == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			schema.SafeLogger(kc.Logger).Error("OnRateLimit hook panicked", "panic", r)
		}
	}()
	kc.OnRateLimit(keyID)
}

func (kc *KeyCache) safeCallOnRevoked(keyID string) {
	if kc.OnRevoked == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			schema.SafeLogger(kc.Logger).Error("OnRevoked hook panicked", "panic", r)
		}
	}()
	kc.OnRevoked(keyID)
}

// extractBearer extracts the token from an "Authorization: Bearer <token>" header.
// Returns the token and true on success, or empty string and false if the header
// is missing, malformed, or the token portion is empty.
func extractBearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if len(h) < 8 || !strings.EqualFold(h[:7], "bearer ") {
		return "", false
	}
	token := strings.TrimSpace(h[7:])
	if token == "" {
		return "", false
	}
	return token, true
}

// writeJSONError writes a JSON error response with the given status code.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// Package db provides database configuration for trackr7.
//
// DBConfig controls how trackr7 talks to the user's Postgres database:
// table names, column names, connection pooling. All names are configurable
// so the library works with arbitrary user schemas.
//
// trackr7 does not quote identifiers in generated SQL.
// Only lowercase snake_case identifiers are supported (^[a-z0-9_.]+$).
// Dots are allowed for schema-qualified table names (e.g. "trackr.locations").
//
// trackr7 does not validate schema correctness at runtime.
// Incorrect column mapping leads to undefined behavior.
package db

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/TheRealShek/trackr7/schema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// identifierRe defines the allowed pattern for all table and column names.
// Lowercase alphanumeric, underscores, and dots (for schema qualification).
// trackr7 interpolates these directly into SQL — no quoting.
var identifierRe = regexp.MustCompile(`^[a-z0-9_.]+$`)

// DBConfig holds all database-related configuration for trackr7.
// Use WithDefaults() to fill zero-value fields, then Validate() to check.
type DBConfig struct {
	// Pool is the user's pgx connection pool. Required.
	Pool *pgxpool.Pool

	// MaxConns limits how many connections trackr7 may use from the pool.
	// 0 = use Pool as-is. >0 = library creates a sub-pool with that limit (wired in writer, Phase 4).
	MaxConns int

	// Table names. Accept schema-qualified paths (e.g. "myschema.locations").
	// LocationsTable is the target table for location writes.
	LocationsTable string
	// APIKeysTable is the source of API key metadata.
	APIKeysTable string
	// AuditLogTable is the append-only audit log table.
	AuditLogTable string

	// Column name mappings. Zero value = defaults applied by WithDefaults().
	// LocationColumns maps the locations table columns.
	LocationColumns LocationColumnMap
	// APIKeyColumns maps the api_keys table columns.
	APIKeyColumns APIKeyColumnMap
	// AuditLogColumns maps the audit_log table columns.
	AuditLogColumns AuditLogColumnMap
}

// LocationColumnMap maps Go field names to actual column names in the locations table.
type LocationColumnMap struct {
	// UUID maps the uuid column name.
	UUID string
	// EntityID maps the entity_id column name.
	EntityID string
	// EntityType maps the entity_type column name.
	EntityType string
	// Lat maps the lat column name.
	Lat string
	// Lng maps the lng column name.
	Lng string
	// TS maps the ts column name.
	TS string
}

// APIKeyColumnMap maps Go field names to actual column names in the api_keys table.
type APIKeyColumnMap struct {
	// KeyID maps the key_id column name.
	KeyID string
	// KeyHash maps the key_hash column name.
	KeyHash string
	// EntityType maps the entity_type column name.
	EntityType string
	// Revoked maps the revoked column name.
	Revoked string
	// CreatedAt maps the created_at column name.
	CreatedAt string
}

// AuditLogColumnMap maps Go field names to actual column names in the audit_log table.
type AuditLogColumnMap struct {
	// KeyID maps the key_id column name.
	KeyID string
	// Action maps the action column name.
	Action string
	// TS maps the ts column name.
	TS string
}

// withDefault returns val if non-empty, otherwise fallback.
func withDefault(val, fallback string) string {
	if val == "" {
		return fallback
	}
	return val
}

// WithDefaults returns a copy of c with all zero-value strings filled to their
// default names. Does not mutate the receiver. Each consuming package should
// call this during its own initialization.
func (c DBConfig) WithDefaults() DBConfig {
	c.LocationsTable = withDefault(c.LocationsTable, "locations")
	c.APIKeysTable = withDefault(c.APIKeysTable, "api_keys")
	c.AuditLogTable = withDefault(c.AuditLogTable, "audit_log")

	c.LocationColumns.UUID = withDefault(c.LocationColumns.UUID, "uuid")
	c.LocationColumns.EntityID = withDefault(c.LocationColumns.EntityID, "entity_id")
	c.LocationColumns.EntityType = withDefault(c.LocationColumns.EntityType, "entity_type")
	c.LocationColumns.Lat = withDefault(c.LocationColumns.Lat, "lat")
	c.LocationColumns.Lng = withDefault(c.LocationColumns.Lng, "lng")
	c.LocationColumns.TS = withDefault(c.LocationColumns.TS, "ts")

	c.APIKeyColumns.KeyID = withDefault(c.APIKeyColumns.KeyID, "key_id")
	c.APIKeyColumns.KeyHash = withDefault(c.APIKeyColumns.KeyHash, "key_hash")
	c.APIKeyColumns.EntityType = withDefault(c.APIKeyColumns.EntityType, "entity_type")
	c.APIKeyColumns.Revoked = withDefault(c.APIKeyColumns.Revoked, "revoked")
	c.APIKeyColumns.CreatedAt = withDefault(c.APIKeyColumns.CreatedAt, "created_at")

	c.AuditLogColumns.KeyID = withDefault(c.AuditLogColumns.KeyID, "key_id")
	c.AuditLogColumns.Action = withDefault(c.AuditLogColumns.Action, "action")
	c.AuditLogColumns.TS = withDefault(c.AuditLogColumns.TS, "ts")

	return c
}

// Validate checks the DBConfig after defaults have been applied.
// Returns schema.ErrInvalidConfig wrapping a description of the first problem found.
//
// Checks performed:
//   - Pool must not be nil
//   - All table and column names must be non-empty after trimming
//   - All table and column names must match ^[a-z0-9_.]+$
//   - No duplicate column names within a single table mapping
func (c DBConfig) Validate() error {
	if c.Pool == nil {
		return fmt.Errorf("%w: Pool is nil", schema.ErrInvalidConfig)
	}

	// Collect all names to validate: label → value.
	tables := []struct {
		label string
		name  string
	}{
		{"LocationsTable", c.LocationsTable},
		{"APIKeysTable", c.APIKeysTable},
		{"AuditLogTable", c.AuditLogTable},
	}

	for _, t := range tables {
		if err := validateIdentifier(t.label, t.name); err != nil {
			return err
		}
	}

	// Validate each column mapping for empty, regex, and duplicates.
	if err := validateColumnSet("LocationColumns", []namedCol{
		{"UUID", c.LocationColumns.UUID},
		{"EntityID", c.LocationColumns.EntityID},
		{"EntityType", c.LocationColumns.EntityType},
		{"Lat", c.LocationColumns.Lat},
		{"Lng", c.LocationColumns.Lng},
		{"TS", c.LocationColumns.TS},
	}); err != nil {
		return err
	}

	if err := validateColumnSet("APIKeyColumns", []namedCol{
		{"KeyID", c.APIKeyColumns.KeyID},
		{"KeyHash", c.APIKeyColumns.KeyHash},
		{"EntityType", c.APIKeyColumns.EntityType},
		{"Revoked", c.APIKeyColumns.Revoked},
		{"CreatedAt", c.APIKeyColumns.CreatedAt},
	}); err != nil {
		return err
	}

	if err := validateColumnSet("AuditLogColumns", []namedCol{
		{"KeyID", c.AuditLogColumns.KeyID},
		{"Action", c.AuditLogColumns.Action},
		{"TS", c.AuditLogColumns.TS},
	}); err != nil {
		return err
	}

	return nil
}

// namedCol pairs a Go field label with its configured column name for validation.
type namedCol struct {
	label string
	value string
}

// validateIdentifier checks that name is non-empty and matches the identifier regex.
func validateIdentifier(label, name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return fmt.Errorf("%w: %s is empty (did you forget to call WithDefaults()?)", schema.ErrInvalidConfig, label)
	}
	if !identifierRe.MatchString(trimmed) {
		return fmt.Errorf("%w: %s %q is not a valid identifier (must match %s)",
			schema.ErrInvalidConfig, label, name, identifierRe.String())
	}
	return nil
}

// validateColumnSet checks all columns in a mapping for validity and duplicates.
func validateColumnSet(setLabel string, cols []namedCol) error {
	seen := make(map[string]string, len(cols)) // column name → first Go field that used it
	for _, col := range cols {
		fullLabel := setLabel + "." + col.label
		if err := validateIdentifier(fullLabel, col.value); err != nil {
			return err
		}
		trimmed := strings.TrimSpace(col.value)
		if prev, dup := seen[trimmed]; dup {
			return fmt.Errorf("%w: %s.%s and %s.%s both map to column %q",
				schema.ErrInvalidConfig, setLabel, prev, setLabel, col.label, trimmed)
		}
		seen[trimmed] = col.label
	}
	return nil
}

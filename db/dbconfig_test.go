package db

import (
	"errors"
	"testing"

	"github.com/TheRealShek/trackr7/schema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// stubPool returns a non-nil *pgxpool.Pool for tests that need Validate() to
// pass the nil-pool check. The pool is not connected — only its address matters.
//
// pgxpool.Pool is a struct, so a non-nil pointer satisfies the check without
// requiring a live database. No methods are called on it.
var stubPool = &pgxpool.Pool{}

func TestWithDefaults(t *testing.T) {
	tests := []struct {
		name   string
		input  DBConfig
		check  func(t *testing.T, got DBConfig)
	}{
		{
			name:  "zero value fills all 17 defaults",
			input: DBConfig{},
			check: func(t *testing.T, got DBConfig) {
				t.Helper()
				assertEq(t, "LocationsTable", got.LocationsTable, "locations")
				assertEq(t, "APIKeysTable", got.APIKeysTable, "api_keys")
				assertEq(t, "AuditLogTable", got.AuditLogTable, "audit_log")

				assertEq(t, "LocationColumns.UUID", got.LocationColumns.UUID, "uuid")
				assertEq(t, "LocationColumns.EntityID", got.LocationColumns.EntityID, "entity_id")
				assertEq(t, "LocationColumns.EntityType", got.LocationColumns.EntityType, "entity_type")
				assertEq(t, "LocationColumns.Lat", got.LocationColumns.Lat, "lat")
				assertEq(t, "LocationColumns.Lng", got.LocationColumns.Lng, "lng")
				assertEq(t, "LocationColumns.TS", got.LocationColumns.TS, "ts")

				assertEq(t, "APIKeyColumns.KeyID", got.APIKeyColumns.KeyID, "key_id")
				assertEq(t, "APIKeyColumns.KeyHash", got.APIKeyColumns.KeyHash, "key_hash")
				assertEq(t, "APIKeyColumns.EntityType", got.APIKeyColumns.EntityType, "entity_type")
				assertEq(t, "APIKeyColumns.Revoked", got.APIKeyColumns.Revoked, "revoked")
				assertEq(t, "APIKeyColumns.CreatedAt", got.APIKeyColumns.CreatedAt, "created_at")

				assertEq(t, "AuditLogColumns.KeyID", got.AuditLogColumns.KeyID, "key_id")
				assertEq(t, "AuditLogColumns.Action", got.AuditLogColumns.Action, "action")
				assertEq(t, "AuditLogColumns.TS", got.AuditLogColumns.TS, "ts")
			},
		},
		{
			name: "partial overrides preserved",
			input: DBConfig{
				LocationsTable: "trackr.pings",
				LocationColumns: LocationColumnMap{
					UUID:     "id",
					EntityID: "device_id",
				},
			},
			check: func(t *testing.T, got DBConfig) {
				t.Helper()
				// Overridden values preserved.
				assertEq(t, "LocationsTable", got.LocationsTable, "trackr.pings")
				assertEq(t, "LocationColumns.UUID", got.LocationColumns.UUID, "id")
				assertEq(t, "LocationColumns.EntityID", got.LocationColumns.EntityID, "device_id")
				// Non-overridden values get defaults.
				assertEq(t, "APIKeysTable", got.APIKeysTable, "api_keys")
				assertEq(t, "LocationColumns.Lat", got.LocationColumns.Lat, "lat")
			},
		},
		{
			name: "all fields set explicitly — nothing changes",
			input: DBConfig{
				LocationsTable: "my_locs",
				APIKeysTable:   "my_keys",
				AuditLogTable:  "my_audit",
				LocationColumns: LocationColumnMap{
					UUID: "a", EntityID: "b", EntityType: "c", Lat: "d", Lng: "e", TS: "f",
				},
				APIKeyColumns: APIKeyColumnMap{
					KeyID: "g", KeyHash: "h", EntityType: "i", Revoked: "j", CreatedAt: "k",
				},
				AuditLogColumns: AuditLogColumnMap{
					KeyID: "l", Action: "m", TS: "n",
				},
			},
			check: func(t *testing.T, got DBConfig) {
				t.Helper()
				assertEq(t, "LocationsTable", got.LocationsTable, "my_locs")
				assertEq(t, "LocationColumns.UUID", got.LocationColumns.UUID, "a")
				assertEq(t, "AuditLogColumns.TS", got.AuditLogColumns.TS, "n")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.WithDefaults()
			tt.check(t, got)
		})
	}
}

func TestValidate(t *testing.T) {
	// valid is a fully-populated config that passes Validate().
	valid := DBConfig{Pool: stubPool}.WithDefaults()

	tests := []struct {
		name    string
		cfg     DBConfig
		wantErr bool
	}{
		{
			name:    "nil pool",
			cfg:     DBConfig{}.WithDefaults(),
			wantErr: true,
		},
		{
			name:    "valid pool with all defaults",
			cfg:     valid,
			wantErr: false,
		},
		{
			name: "schema-qualified table name",
			cfg: func() DBConfig {
				c := valid
				c.LocationsTable = "trackr.locations"
				return c
			}(),
			wantErr: false,
		},
		{
			name: "whitespace-only table name",
			cfg: func() DBConfig {
				c := valid
				c.LocationsTable = "   "
				return c
			}(),
			wantErr: true,
		},
		{
			name: "whitespace-only column name",
			cfg: func() DBConfig {
				c := valid
				c.LocationColumns.UUID = "   "
				return c
			}(),
			wantErr: true,
		},
		{
			name: "duplicate column in LocationColumns",
			cfg: func() DBConfig {
				c := valid
				c.LocationColumns.UUID = "id"
				c.LocationColumns.EntityID = "id"
				return c
			}(),
			wantErr: true,
		},
		{
			name: "duplicate column in APIKeyColumns",
			cfg: func() DBConfig {
				c := valid
				c.APIKeyColumns.KeyID = "col_a"
				c.APIKeyColumns.KeyHash = "col_a"
				return c
			}(),
			wantErr: true,
		},
		{
			name: "uppercase table name",
			cfg: func() DBConfig {
				c := valid
				c.LocationsTable = "Locations"
				return c
			}(),
			wantErr: true,
		},
		{
			name: "uppercase column name",
			cfg: func() DBConfig {
				c := valid
				c.LocationColumns.EntityID = "EntityId"
				return c
			}(),
			wantErr: true,
		},
		{
			name: "quoted identifier",
			cfg: func() DBConfig {
				c := valid
				c.LocationsTable = `"locations"`
				return c
			}(),
			wantErr: true,
		},
		{
			name: "semicolon in name",
			cfg: func() DBConfig {
				c := valid
				c.LocationsTable = "locations;"
				return c
			}(),
			wantErr: true,
		},
		{
			name: "space in column name",
			cfg: func() DBConfig {
				c := valid
				c.LocationColumns.EntityID = "entity id"
				return c
			}(),
			wantErr: true,
		},
		{
			name: "valid snake_case",
			cfg: func() DBConfig {
				c := valid
				c.LocationColumns.EntityID = "device_id"
				return c
			}(),
			wantErr: false,
		},
		{
			name: "schema-qualified with dot",
			cfg: func() DBConfig {
				c := valid
				c.LocationsTable = "myschema.my_table"
				return c
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()

			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr && !errors.Is(err, schema.ErrInvalidConfig) {
				t.Errorf("error should wrap ErrInvalidConfig, got: %v", err)
			}
		})
	}
}

func assertEq(t *testing.T, field, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %q, want %q", field, got, want)
	}
}

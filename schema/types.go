// Package schema defines the canonical data types and exported errors for trackr7.
//
// trackr7 does not own database schema — the user creates and manages tables.
// Reference SQL is provided at schema/migrations/001_init.sql for convenience.
package schema

// Location represents a single location ping stored in the locations table.
// Fields map 1:1 to DB columns. TS is server-stamped UTC milliseconds,
// used for display only — never for ordering guarantees.
type Location struct {
	// UUID is the client-generated unique identifier for a ping.
	UUID string `json:"uuid"`
	// EntityID is the user-defined identifier used for partitioning.
	EntityID string `json:"entity_id"`
	// EntityType is the category assigned to the entity (e.g. vehicle).
	EntityType string `json:"entity_type"`
	// Lat is the latitude in decimal degrees.
	Lat float64 `json:"lat"`
	// Lng is the longitude in decimal degrees.
	Lng float64 `json:"lng"`
	// TS is the server-stamped UTC time in milliseconds.
	TS int64 `json:"ts"`
}

// KeyInfo is the public-facing view of an API key.
// key_hash is deliberately excluded — it never leaves the admin package.
type KeyInfo struct {
	// KeyID is the UUID for the API key.
	KeyID string `json:"key_id"`
	// EntityType is the associated entity type for the key.
	EntityType string `json:"entity_type"`
	// Revoked indicates whether the key has been revoked.
	Revoked bool `json:"revoked"`
	// CreatedAt is the server-stamped UTC time in milliseconds.
	CreatedAt int64 `json:"created_at"`
}

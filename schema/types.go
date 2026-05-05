// Package schema defines the canonical data types and exported errors for trackr7.
//
// trackr7 does not own database schema — the user creates and manages tables.
// Reference SQL is provided at schema/migrations/001_init.sql for convenience.
package schema

// Location represents a single location ping stored in the locations table.
// Fields map 1:1 to DB columns. TS is server-stamped UTC milliseconds,
// used for display only — never for ordering guarantees.
type Location struct {
	UUID       string  `json:"uuid"`
	EntityID   string  `json:"entity_id"`
	EntityType string  `json:"entity_type"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	TS         int64   `json:"ts"`
}

// KeyInfo is the public-facing view of an API key.
// key_hash is deliberately excluded — it never leaves the admin package.
type KeyInfo struct {
	KeyID      string `json:"key_id"`
	EntityType string `json:"entity_type"`
	Revoked    bool   `json:"revoked"`
	CreatedAt  int64  `json:"created_at"`
}

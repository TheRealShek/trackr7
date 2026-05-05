package schema

import "errors"

// ErrUnauthorized is returned when a request fails authentication.
// Callers can match with errors.Is(err, ErrUnauthorized).
var ErrUnauthorized = errors.New("trackr7: unauthorized")

// ErrRateLimited is returned when a key exceeds its rate limit.
var ErrRateLimited = errors.New("trackr7: rate limited")

// ErrNamespaceRequired is returned when a required Namespace config field is empty.
var ErrNamespaceRequired = errors.New("trackr7: namespace required")

// ErrInvalidConfig is returned when DBConfig validation fails (nil pool,
// empty names, duplicate columns, or invalid identifiers).
var ErrInvalidConfig = errors.New("trackr7: invalid config")

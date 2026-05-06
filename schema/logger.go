package schema

// Logger is the logging interface used by trackr7 packages.
// All packages that accept a Logger treat nil as valid — passing nil
// silently disables logging. Use SafeLogger to wrap a potentially nil
// Logger into a no-op implementation.
type Logger interface {
	Info(msg string, fields ...any)
	Error(msg string, fields ...any)
}

// nopLogger is a no-op Logger returned by SafeLogger when given nil.
type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// SafeLogger returns l if non-nil, otherwise returns a silent no-op Logger.
func SafeLogger(l Logger) Logger {
	if l == nil {
		return nopLogger{}
	}
	return l
}

package domain

// ErrorCode is the fixed, machine-readable error vocabulary the brief requires
// (DESIGN.md §4). These values go on the wire verbatim inside the error envelope's
// "code" field, so a client can branch on them without parsing a message string.
type ErrorCode string

const (
	ErrTenantNotFound        ErrorCode = "tenant_not_found"
	ErrTenantAlreadyExists   ErrorCode = "tenant_already_exists"
	ErrTenantUpdateNotAllowed ErrorCode = "tenant_update_not_allowed"
	ErrTenantVersionConflict ErrorCode = "tenant_version_conflict"
	ErrTaskNotFound          ErrorCode = "task_not_found"
	ErrValidation            ErrorCode = "validation_error"
	ErrMalformedRequest      ErrorCode = "malformed_request"
	ErrInternal              ErrorCode = "internal_error"
)

// Error is a structured domain error. It carries the stable Code plus a human
// Message, so the httpapi layer can map Code -> HTTP status (DESIGN.md §4) without
// the domain layer knowing anything about HTTP.
type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

func NewError(code ErrorCode, message string) *Error {
	return &Error{Code: code, Message: message}
}

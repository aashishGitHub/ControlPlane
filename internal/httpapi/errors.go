package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/aashishGithub/controlplane/internal/domain"
)

// errorStatus is the DESIGN.md §4 code -> HTTP status table, kept as one map so
// adding a new domain.ErrorCode means one new line here, not a new case
// scattered across handlers.
var errorStatus = map[domain.ErrorCode]int{
	domain.ErrTenantNotFound:         http.StatusNotFound,
	domain.ErrTaskNotFound:           http.StatusNotFound,
	domain.ErrTenantAlreadyExists:    http.StatusConflict,
	domain.ErrTenantUpdateNotAllowed: http.StatusConflict,
	domain.ErrTenantVersionConflict:  http.StatusConflict,
	domain.ErrValidation:             http.StatusUnprocessableEntity,
	domain.ErrMalformedRequest:       http.StatusBadRequest,
	domain.ErrInternal:               http.StatusInternalServerError,
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code      domain.ErrorCode `json:"code"`
	Message   string           `json:"message"`
	Details   map[string]any   `json:"details,omitempty"`
	RequestID string           `json:"request_id"`
}

// writeError renders err as the DESIGN.md §4 error envelope. Any error that
// isn't a *domain.Error (a store/driver failure that reached here unmapped) is
// logged with its real cause and sent to the client as a bare internal_error -
// the client never sees driver/SQL details.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	var derr *domain.Error
	if !errors.As(err, &derr) {
		slog.ErrorContext(r.Context(), "unmapped error", "error", err)
		derr = domain.NewError(domain.ErrInternal, "internal error")
	}
	status, ok := errorStatus[derr.Code]
	if !ok {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, errorEnvelope{Error: errorBody{
		Code:      derr.Code,
		Message:   derr.Message,
		RequestID: middleware.GetReqID(r.Context()),
	}})
}

// validationError is a 422 with field-level details, used by request decoding
// (DESIGN.md §4: validation_error carries "details").
func validationError(w http.ResponseWriter, r *http.Request, details map[string]any) {
	writeJSON(w, http.StatusUnprocessableEntity, errorEnvelope{Error: errorBody{
		Code:      domain.ErrValidation,
		Message:   "request failed validation",
		Details:   details,
		RequestID: middleware.GetReqID(r.Context()),
	}})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

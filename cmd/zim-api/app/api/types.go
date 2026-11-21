package api

// Response is the REST success/failure envelope.
type Response struct {
	Code      int    `json:"code"`
	Message   string `json:"message,omitempty"`
	Data      any    `json:"data,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

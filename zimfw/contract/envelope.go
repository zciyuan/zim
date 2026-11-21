package contract

import (
	"github.com/dobyte/due/v2/errors"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// ErrInvalidEnvelope reports a malformed protobuf envelope.
var ErrInvalidEnvelope = errors.New("invalid zim envelope")

// NewEnvelope builds a request or success response envelope.
func NewEnvelope(opcode int32, payload []byte) *Envelope {
	return &Envelope{Opcode: opcode, Payload: payload}
}

// EncodeEnvelope marshals an envelope.
func EncodeEnvelope(envelope *Envelope) ([]byte, error) {
	if envelope == nil {
		return nil, ErrInvalidEnvelope
	}
	payload, err := proto.Marshal(envelope)
	if err != nil {
		return nil, ErrInvalidEnvelope
	}
	return payload, nil
}

// DecodeEnvelope unmarshals an envelope and verifies its opcode.
func DecodeEnvelope(payload []byte, opcode int32) (*Envelope, error) {
	envelope := &Envelope{}
	if err := proto.Unmarshal(payload, envelope); err != nil {
		return nil, ErrInvalidEnvelope
	}
	if envelope.Opcode == 0 {
		envelope.Opcode = opcode
	}
	if envelope.Opcode != opcode {
		return nil, ErrInvalidEnvelope
	}
	return envelope, nil
}

// ErrorEnvelope builds a response envelope from a business error.
func ErrorEnvelope(opcode int32, err error) *Envelope {
	business := errs.From(err)
	if business == nil {
		return NewEnvelope(opcode, nil)
	}
	return &Envelope{
		Opcode:  opcode,
		Code:    int32(business.Code().Code()),
		Message: business.Message(),
	}
}

// ResponseEnvelope builds a success response envelope.
func ResponseEnvelope(opcode int32, payload []byte) *Envelope {
	return &Envelope{Opcode: opcode, Payload: payload}
}

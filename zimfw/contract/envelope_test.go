package contract

import (
	"testing"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	want := &Envelope{Opcode: OpLogin, Payload: []byte("payload"), Code: 0, Message: ""}
	raw, err := EncodeEnvelope(want)
	if err != nil {
		t.Fatalf("EncodeEnvelope: %v", err)
	}
	got, err := DecodeEnvelope(raw, OpLogin)
	if err != nil {
		t.Fatalf("DecodeEnvelope: %v", err)
	}
	if got.Opcode != want.Opcode || string(got.Payload) != string(want.Payload) {
		t.Fatalf("got %+v", got)
	}
}

func TestDecodeEnvelopeDefaultsOpcode(t *testing.T) {
	raw, _ := EncodeEnvelope(&Envelope{Payload: []byte("x")})
	got, err := DecodeEnvelope(raw, OpGetProfile)
	if err != nil {
		t.Fatalf("DecodeEnvelope: %v", err)
	}
	if got.Opcode != OpGetProfile {
		t.Fatalf("opcode = %d", got.Opcode)
	}
}

func TestDecodeEnvelopeErrors(t *testing.T) {
	if _, err := EncodeEnvelope(nil); err != ErrInvalidEnvelope {
		t.Fatalf("EncodeEnvelope(nil) err = %v", err)
	}
	if _, err := DecodeEnvelope([]byte{0xff, 0xff, 0xff}, OpLogin); err != ErrInvalidEnvelope {
		t.Fatalf("malformed payload err = %v", err)
	}
	raw, _ := EncodeEnvelope(&Envelope{Opcode: OpLogout})
	if _, err := DecodeEnvelope(raw, OpLogin); err != ErrInvalidEnvelope {
		t.Fatalf("opcode mismatch err = %v", err)
	}
}

func TestErrorEnvelope(t *testing.T) {
	envelope := ErrorEnvelope(OpCreateGroup, errs.ErrGroupNotExist)
	if envelope.Opcode != OpCreateGroup || envelope.Code != 130001 || envelope.Message != "group not exist" {
		t.Fatalf("envelope = %+v", envelope)
	}
	if len(envelope.Payload) != 0 {
		t.Fatalf("payload = %v", envelope.Payload)
	}

	envelope = ErrorEnvelope(OpLogin, nil)
	if envelope.Code != 0 || envelope.Message != "" {
		t.Fatalf("nil error envelope = %+v", envelope)
	}
}

func TestResponseEnvelope(t *testing.T) {
	envelope := ResponseEnvelope(OpLogin, []byte("ok"))
	if envelope.Opcode != OpLogin || envelope.Code != 0 || string(envelope.Payload) != "ok" {
		t.Fatalf("envelope = %+v", envelope)
	}
}

package contract

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	body := []byte{1, 2, 3, 250}
	raw, err := EncodeFrame(Frame{Version: Version1, Opcode: OpLogin, Seq: 42, Body: body})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if len(raw) != HeaderSize+len(body) {
		t.Fatalf("len = %d, want %d", len(raw), HeaderSize+len(body))
	}
	if got := binary.BigEndian.Uint32(raw[:4]); got != uint32(MinFrameLength+len(body)) {
		t.Fatalf("length prefix = %d", got)
	}
	if raw[4] != Version1 {
		t.Fatalf("version = %d", raw[4])
	}
	if got := binary.BigEndian.Uint16(raw[5:7]); got != uint16(OpLogin) {
		t.Fatalf("opcode = %d", got)
	}
	if got := binary.BigEndian.Uint32(raw[7:11]); got != 42 {
		t.Fatalf("seq = %d", got)
	}

	frame, err := DecodeFrame(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if frame.Version != Version1 || frame.Opcode != OpLogin || frame.Seq != 42 || !bytes.Equal(frame.Body, body) {
		t.Fatalf("decoded = %+v", frame)
	}
}

func TestEncodeFrameDefaultsVersion(t *testing.T) {
	raw, err := EncodeFrame(Frame{Opcode: OpLogout})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if raw[4] != Version1 {
		t.Fatalf("version = %d", raw[4])
	}
}

func TestEncodeFrameErrors(t *testing.T) {
	cases := []struct {
		name  string
		frame Frame
		want  error
	}{
		{name: "version", frame: Frame{Version: 2, Opcode: 1}, want: ErrInvalidFrame},
		{name: "negative opcode", frame: Frame{Opcode: -1}, want: ErrInvalidFrame},
		{name: "opcode overflow", frame: Frame{Opcode: 0x10000}, want: ErrInvalidFrame},
		{name: "body too large", frame: Frame{Opcode: 1, Body: make([]byte, MaxBodySize+1)}, want: ErrFrameTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := EncodeFrame(tc.frame); err != tc.want {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeFrameErrors(t *testing.T) {
	valid, _ := EncodeFrame(Frame{Opcode: 1})

	cases := []struct {
		name string
		raw  []byte
		want error
	}{
		{name: "short header", raw: []byte{0, 0}, want: io.ErrUnexpectedEOF},
		{name: "length too small", raw: []byte{0, 0, 0, 1}, want: ErrInvalidFrame},
		{name: "length too large", raw: []byte{0, 1, 0, 8}, want: ErrFrameTooLarge},
		{name: "version", raw: append([]byte(nil), valid...), want: ErrInvalidFrame},
		{name: "length mismatch", raw: valid[:len(valid)-1], want: io.ErrUnexpectedEOF},
	}
	cases[3].raw[4] = 2

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeFrame(bytes.NewReader(tc.raw)); err != tc.want {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeFrameBytesTruncatedBody(t *testing.T) {
	raw, _ := EncodeFrame(Frame{Opcode: 1, Body: []byte{1, 2}})
	if _, err := DecodeFrame(bytes.NewReader(raw[:len(raw)-1])); err == nil {
		t.Fatal("expected an error for a truncated body")
	}
}

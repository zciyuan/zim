package contract

import (
	"bytes"
	"testing"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/packet"
)

func TestPackerPackAndUnpack(t *testing.T) {
	packer := NewPacker()
	body := []byte("envelope")
	packed, err := packer.PackMessage(OpLogin, 7, body)
	if err != nil {
		t.Fatalf("PackMessage: %v", err)
	}
	defer packed.Release()

	route, seq, err := packer.ExtractRouteSeq(packed)
	if err != nil {
		t.Fatalf("ExtractRouteSeq: %v", err)
	}
	if route != OpLogin || seq != 7 {
		t.Fatalf("route/seq = %d/%d", route, seq)
	}

	gotRoute, gotSeq, message, err := packer.UnpackMessage(packed)
	if err != nil {
		t.Fatalf("UnpackMessage: %v", err)
	}
	if gotRoute != OpLogin || gotSeq != 7 || !bytes.Equal(message.Bytes(), body) {
		t.Fatalf("unpacked = %d/%d/%q", gotRoute, gotSeq, message.Bytes())
	}

	extractRoute, extractSeq, extractBody, err := packer.ExtractMessage(packed)
	if err != nil {
		t.Fatalf("ExtractMessage: %v", err)
	}
	if extractRoute != OpLogin || extractSeq != 7 || !bytes.Equal(extractBody, body) {
		t.Fatalf("extracted = %d/%d/%q", extractRoute, extractSeq, extractBody)
	}
}

func TestPackerRead(t *testing.T) {
	raw, _ := EncodeFrame(Frame{Opcode: OpSendSingle, Seq: 9, Body: []byte("hello")})
	frameType, heartbeatTime, buf, err := NewPacker().Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if frameType != packet.FrameData || heartbeatTime != 0 {
		t.Fatalf("frame type = %v, heartbeat = %d", frameType, heartbeatTime)
	}
	if !bytes.Equal(buf.Bytes(), raw) {
		t.Fatalf("buffer mismatch")
	}
}

func TestPackerReadHeartbeat(t *testing.T) {
	raw, _ := EncodeFrame(Frame{})
	frameType, _, buf, err := NewPacker().Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if frameType != packet.FrameHeartbeat {
		t.Fatalf("frame type = %v", frameType)
	}
	if buf != nil {
		t.Fatalf("heartbeat buffer = %v", buf)
	}
}

func TestPackerReadErrors(t *testing.T) {
	cases := [][]byte{
		{0, 0},
		{0, 0, 0, 1},
		{0, 1, 0, 0},
	}
	for _, raw := range cases {
		if _, _, _, err := NewPacker().Read(bytes.NewReader(raw)); err == nil {
			t.Fatalf("expected error for %v", raw)
		}
	}

	raw, _ := EncodeFrame(Frame{Opcode: 1, Body: []byte{1}})
	if _, _, _, err := NewPacker().Read(bytes.NewReader(raw[:len(raw)-1])); err == nil {
		t.Fatal("expected truncated body error")
	}
}

func TestPackerPackMessageErrors(t *testing.T) {
	if _, err := NewPacker().PackMessage(-1, 1, nil); err == nil {
		t.Fatal("expected negative opcode error")
	}
	if _, err := NewPacker().PackMessage(1, 1, make([]byte, MaxBodySize+1)); err == nil {
		t.Fatal("expected oversize error")
	}
}

func TestPackerUnsupportedHandshake(t *testing.T) {
	if _, err := NewPacker().PackHandshake(nil); !errors.Is(err, errors.ErrInvalidMessage) {
		t.Fatalf("PackHandshake err = %v", err)
	}
	if _, err := NewPacker().ExtractHandshake(nil); !errors.Is(err, errors.ErrInvalidMessage) {
		t.Fatalf("ExtractHandshake err = %v", err)
	}
}

func TestPackerPackHeartbeat(t *testing.T) {
	raw := NewPacker().PackHeartbeat().Bytes()
	if len(raw) != HeaderSize || raw[0] != 0 || raw[3] != MinFrameLength || raw[4] != Version1 {
		t.Fatalf("heartbeat = %v", raw)
	}
}

func TestPackerNilBuffer(t *testing.T) {
	if _, _, err := NewPacker().ExtractRouteSeq(nil); err != ErrInvalidFrame {
		t.Fatalf("ExtractRouteSeq(nil) err = %v", err)
	}
	if _, _, _, err := NewPacker().UnpackMessage(nil); err != ErrInvalidFrame {
		t.Fatalf("UnpackMessage(nil) err = %v", err)
	}
	if _, _, _, err := NewPacker().ExtractMessage(nil); err != ErrInvalidFrame {
		t.Fatalf("ExtractMessage(nil) err = %v", err)
	}
	if got := NewPacker().MessageBufferHeadroom(); got != 0 {
		t.Fatalf("headroom = %d", got)
	}
}

func TestPackerInvalidBuffer(t *testing.T) {
	buf := buffer.NewBytes([]byte{1, 2, 3}, true)
	if _, _, err := NewPacker().ExtractRouteSeq(buf); err == nil {
		t.Fatal("expected error")
	}
}

package contract

import (
	"encoding/binary"
	"io"

	"github.com/dobyte/due/v2/errors"
)

const (
	// Version1 is the current ZIM protocol version.
	Version1 uint8 = 1
	// SizeBytes is the width of the length prefix.
	SizeBytes = 4
	// HeaderSize is the fixed frame header size, including the length prefix.
	HeaderSize = 11
	// BodyOffset is the offset of the protobuf body in a frame.
	BodyOffset = HeaderSize
	// MinFrameLength is the value of the length prefix for an empty body.
	MinFrameLength = HeaderSize - SizeBytes
	// MaxBodySize is the maximum protobuf body size (64 KiB).
	MaxBodySize = 64 * 1024
	// MaxFrameLength is the maximum value of the length prefix.
	MaxFrameLength = MinFrameLength + MaxBodySize
)

// ErrInvalidFrame reports a malformed or unsupported frame.
var ErrInvalidFrame = errors.New("invalid zim frame")

// ErrFrameTooLarge reports a frame whose body exceeds MaxBodySize.
var ErrFrameTooLarge = errors.New("zim frame too large")

// EncodeFrame validates frame and encodes it as [len4][ver1][op2][seq4][body].
func EncodeFrame(frame Frame) ([]byte, error) {
	if frame.Version == 0 {
		frame.Version = Version1
	}
	if frame.Version != Version1 {
		return nil, ErrInvalidFrame
	}
	if frame.Opcode < 0 || frame.Opcode > 0xFFFF {
		return nil, ErrInvalidFrame
	}
	if len(frame.Body) > MaxBodySize {
		return nil, ErrFrameTooLarge
	}

	raw := make([]byte, HeaderSize+len(frame.Body))
	binary.BigEndian.PutUint32(raw[0:4], uint32(MinFrameLength+len(frame.Body)))
	raw[4] = frame.Version
	binary.BigEndian.PutUint16(raw[5:7], uint16(frame.Opcode))
	binary.BigEndian.PutUint32(raw[7:11], frame.Seq)
	copy(raw[HeaderSize:], frame.Body)
	return raw, nil
}

// DecodeFrame reads exactly one frame from r.
func DecodeFrame(r io.Reader) (*Frame, error) {
	var size [SizeBytes]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(size[:])
	if length < MinFrameLength {
		return nil, ErrInvalidFrame
	}
	if length > MaxFrameLength {
		return nil, ErrFrameTooLarge
	}

	raw := make([]byte, SizeBytes+int(length))
	copy(raw[:SizeBytes], size[:])
	if _, err := io.ReadFull(r, raw[SizeBytes:]); err != nil {
		return nil, err
	}
	return DecodeFrameBytes(raw)
}

// DecodeFrameBytes decodes one complete frame.
func DecodeFrameBytes(raw []byte) (*Frame, error) {
	if len(raw) < HeaderSize {
		return nil, ErrInvalidFrame
	}

	length := binary.BigEndian.Uint32(raw[:SizeBytes])
	if length < MinFrameLength {
		return nil, ErrInvalidFrame
	}
	if length > MaxFrameLength {
		return nil, ErrFrameTooLarge
	}
	if int(length) != len(raw)-SizeBytes {
		return nil, ErrInvalidFrame
	}
	if raw[4] != Version1 {
		return nil, ErrInvalidFrame
	}

	opcode := int32(binary.BigEndian.Uint16(raw[5:7]))
	seq := binary.BigEndian.Uint32(raw[7:11])
	body := raw[HeaderSize:]
	return &Frame{Version: raw[4], Opcode: opcode, Seq: seq, Body: body}, nil
}

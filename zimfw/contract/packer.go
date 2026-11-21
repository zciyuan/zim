package contract

import (
	"encoding/binary"
	"io"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/packet"
)

var _ packet.Packer = (*Packer)(nil)

// NewPacker returns a ZIM protocol packer.
func NewPacker() *Packer {
	return &Packer{}
}

// InstallPacker replaces due's global packet codec with the ZIM codec. It must
// be called before any due component starts.
func InstallPacker() {
	packet.SetPacker(NewPacker())
}

// Read reads one ZIM frame. Heartbeats are represented by a frame with opcode
// zero, sequence zero and an empty body; they are only used by due's transport
// heartbeat and never by a ZIM client.
func (p *Packer) Read(reader io.Reader) (packet.FrameType, int64, buffer.Buffer, error) {
	var size [SizeBytes]byte
	if _, err := io.ReadFull(reader, size[:]); err != nil {
		return packet.FrameData, 0, nil, err
	}

	length := binary.BigEndian.Uint32(size[:])
	if length < MinFrameLength {
		return packet.FrameData, 0, nil, ErrInvalidFrame
	}
	if length > MaxFrameLength {
		return packet.FrameData, 0, nil, ErrFrameTooLarge
	}

	raw := make([]byte, SizeBytes+int(length))
	copy(raw[:SizeBytes], size[:])
	if _, err := io.ReadFull(reader, raw[SizeBytes:]); err != nil {
		return packet.FrameData, 0, nil, err
	}

	frame, err := DecodeFrameBytes(raw)
	if err != nil {
		return packet.FrameData, 0, nil, err
	}
	if frame.Opcode == 0 && frame.Seq == 0 && len(frame.Body) == 0 {
		return packet.FrameHeartbeat, 0, nil, nil
	}
	return packet.FrameData, 0, buffer.NewBytes(raw, true), nil
}

// PackMessage encodes an envelope into a ZIM frame.
func (p *Packer) PackMessage(route, seq int32, data []byte) (buffer.Buffer, error) {
	frame, err := EncodeFrame(Frame{
		Version: Version1,
		Opcode:  route,
		Seq:     uint32(seq),
		Body:    data,
	})
	if err != nil {
		return nil, err
	}
	return buffer.NewBytes(frame, true), nil
}

// PackHandshake is unsupported: ZIM authenticates during the HTTP upgrade and
// does not enable due session security.
func (p *Packer) PackHandshake([]byte) (buffer.Buffer, error) {
	return nil, errors.ErrInvalidMessage
}

// ExtractHandshake is unsupported.
func (p *Packer) ExtractHandshake(buffer.Buffer) ([]byte, error) {
	return nil, errors.ErrInvalidMessage
}

// PackHeartbeat returns the internal due heartbeat frame.
func (p *Packer) PackHeartbeat(server ...bool) buffer.Buffer {
	frame, _ := EncodeFrame(Frame{Version: Version1})
	return buffer.NewBytes(frame, true)
}

// UnpackMessage returns the Envelope body of buf.
func (p *Packer) UnpackMessage(buf buffer.Buffer) (int32, int32, buffer.Buffer, error) {
	if buf == nil {
		return 0, 0, nil, ErrInvalidFrame
	}
	frame, err := DecodeFrameBytes(buf.Bytes())
	if err != nil {
		return 0, 0, nil, err
	}
	return frame.Opcode, int32(frame.Seq), buffer.NewBytes(frame.Body, true), nil
}

// ExtractRouteSeq returns the outer opcode and sequence of buf.
func (p *Packer) ExtractRouteSeq(buf buffer.Buffer) (int32, int32, error) {
	if buf == nil {
		return 0, 0, ErrInvalidFrame
	}
	frame, err := DecodeFrameBytes(buf.Bytes())
	if err != nil {
		return 0, 0, err
	}
	return frame.Opcode, int32(frame.Seq), nil
}

// ExtractMessage returns the Envelope body without copying.
func (p *Packer) ExtractMessage(buf buffer.Buffer) (int32, int32, []byte, error) {
	if buf == nil {
		return 0, 0, nil, ErrInvalidFrame
	}
	frame, err := DecodeFrameBytes(buf.Bytes())
	if err != nil {
		return 0, 0, nil, err
	}
	return frame.Opcode, int32(frame.Seq), frame.Body, nil
}

// MessageBufferHeadroom reports zero: the ZIM header is built after the
// payload is encoded, so due's in-place header optimisation does not apply.
func (p *Packer) MessageBufferHeadroom() int {
	return 0
}

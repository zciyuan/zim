package contract

import (
	"context"
	"sync"
)

// Ctx is the normalized business context passed to every handler. The gateway
// only parses the frame and envelope; it never interprets business fields.
type Ctx struct {
	context.Context

	// UID is the real user ID split from the device key.
	UID int64
	// Platform is the device platform (1 iOS, 2 Android, 3 PC, 4 Web, 5 Pad).
	Platform int32
	// DeviceKey is uid<<11|platform<<8|slot and is the due session identity.
	DeviceKey int64
	// CID is the gateway connection ID.
	CID int64
	// GID is the gateway instance ID.
	GID string
	// Route is the outer opcode.
	Route int32
	// Seq is the client request sequence.
	Seq int32
	// TraceID is the request correlation ID propagated through logs and RPC.
	TraceID string
	// ClientIP is the remote client IP.
	ClientIP string
}

// Frame is one decoded ZIM WebSocket frame.
type Frame struct {
	// Version is the protocol version; it must be Version1.
	Version uint8
	// Opcode is the ZIM opcode.
	Opcode int32
	// Seq is the client request sequence; zero for server pushes.
	Seq uint32
	// Body is the protobuf-encoded Envelope.
	Body []byte
}

// OpcodeSegment classifies an opcode.
type OpcodeSegment struct {
	// Domain is the human readable segment name.
	Domain string
	// Base is the first opcode of the segment.
	Base int32
	// Max is the last opcode of the segment.
	Max int32
}

// Packer implements due's packet.Packer using the frozen ZIM frame format.
//
// It is installed once per process with InstallPacker. Keeping the protocol
// codec in zimfw rather than in a forked due tree is the upgrade boundary
// required by ZIM-01 part 2 §8.
type Packer struct{}

// Router is an immutable-after-start route table. Registration is serialized;
// lookup takes only a read lock.
type Router struct {
	mu     sync.RWMutex
	routes map[int32]HandlerFunc
}

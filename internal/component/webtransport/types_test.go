package webtransport

// receivedFrame is a test-only copy of a decoded ZIM frame.
type receivedFrame struct {
	opcode int32
	seq    uint32
	body   []byte
}

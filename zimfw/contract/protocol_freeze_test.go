package contract

import "testing"

func TestProtocolFreezeOpcodesAndMessageTypes(t *testing.T) {
	for opcode, segment := range AllOpcode {
		actual, ok := SegmentOf(opcode)
		if !ok {
			t.Fatalf("opcode %d has no segment", opcode)
		}
		if actual.Domain != segment.Domain || actual.Base != segment.Base || actual.Max != segment.Max {
			t.Fatalf("opcode %d segment = %+v, want %+v", opcode, actual, segment)
		}
	}
	for msgType := int32(1); msgType <= 7; msgType++ {
		if !IsMessageType(msgType) {
			t.Fatalf("msg_type %d must be valid", msgType)
		}
	}
	for _, msgType := range []int32{20, 21, 22} {
		if !IsMessageType(msgType) {
			t.Fatalf("msg_type %d must be valid", msgType)
		}
	}
	for _, msgType := range []int32{0, 8, 19, 23, 99} {
		if IsMessageType(msgType) {
			t.Fatalf("msg_type %d must be invalid", msgType)
		}
	}
}

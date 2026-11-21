package contract

import "testing"

func TestOpcodeSegments(t *testing.T) {
	for opcode, want := range AllOpcode {
		got, ok := SegmentOf(opcode)
		if !ok {
			t.Fatalf("SegmentOf(%d) = not found", opcode)
		}
		if got.Domain != want.Domain || got.Base != want.Base || got.Max != want.Max {
			t.Fatalf("SegmentOf(%d) = %+v, want %+v", opcode, got, want)
		}
	}
}

func TestSegmentBoundaries(t *testing.T) {
	cases := []struct {
		opcode int32
		want   string
		ok     bool
	}{
		{1000, "auth", true},
		{1099, "auth", true},
		{1100, "user", true},
		{1699, "sync", true},
		{1700, "chatroom", true},
		{1899, "rtc", true},
		{1900, "push", true},
		{1999, "push", true},
		{2000, "", false},
		{8999, "", false},
		{9000, "infra", true},
		{9999, "infra", true},
	}
	for _, tc := range cases {
		got, ok := SegmentOf(tc.opcode)
		if ok != tc.ok || got.Domain != tc.want {
			t.Fatalf("SegmentOf(%d) = (%+v, %v), want (%q, %v)", tc.opcode, got, ok, tc.want, tc.ok)
		}
	}
}

func TestSegmentsReturnsCopy(t *testing.T) {
	first := Segments()
	first[0].Domain = "mutated"
	second := Segments()
	if second[0].Domain != "auth" {
		t.Fatalf("Segments returned shared storage: %q", second[0].Domain)
	}
}

func TestIsPush(t *testing.T) {
	for opcode := int32(1901); opcode <= 1907; opcode++ {
		if !IsPush(opcode) {
			t.Fatalf("IsPush(%d) = false", opcode)
		}
	}
	for _, opcode := range []int32{1801, 1900, 1908, 2000} {
		if IsPush(opcode) {
			t.Fatalf("IsPush(%d) = true", opcode)
		}
	}
}

func TestIsMessageType(t *testing.T) {
	valid := []int32{1, 2, 3, 4, 5, 6, 7, 20, 21, 22}
	for _, msgType := range valid {
		if !IsMessageType(msgType) {
			t.Fatalf("IsMessageType(%d) = false", msgType)
		}
	}
	invalid := []int32{0, 8, 19, 23, -1}
	for _, msgType := range invalid {
		if IsMessageType(msgType) {
			t.Fatalf("IsMessageType(%d) = true", msgType)
		}
	}
}

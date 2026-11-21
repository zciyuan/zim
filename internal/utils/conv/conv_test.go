package conv

import "testing"

func TestSingle(t *testing.T) {
	if got := Single(2, 1); got != "s_1_2" {
		t.Fatalf("Single = %q", got)
	}
	if got := Single(1, 2); got != "s_1_2" {
		t.Fatalf("Single = %q", got)
	}
}

func TestGroup(t *testing.T) {
	if got := Group(42); got != "g_42" {
		t.Fatalf("Group = %q", got)
	}
}

func TestParseSingle(t *testing.T) {
	if a, b, ok := ParseSingle(Single(2, 1)); !ok || a != 1 || b != 2 {
		t.Fatalf("ParseSingle = %d %d %v", a, b, ok)
	}
	for _, bad := range []string{"", "s_", "s_1", "s_1_2_3", "s_a_2", "s_0_2", "s_1_1", "g_1"} {
		if _, _, ok := ParseSingle(bad); ok {
			t.Fatalf("ParseSingle(%q) should fail", bad)
		}
	}
}

func TestParseGroup(t *testing.T) {
	if gid, ok := ParseGroup(Group(42)); !ok || gid != 42 {
		t.Fatalf("ParseGroup = %d %v", gid, ok)
	}
	for _, bad := range []string{"", "g_", "g_x", "g_0", "s_1_2"} {
		if _, ok := ParseGroup(bad); ok {
			t.Fatalf("ParseGroup(%q) should fail", bad)
		}
	}
}

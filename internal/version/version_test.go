package version

import "testing"

func TestString(t *testing.T) {
	if got := String(); got == "" {
		t.Fatal("empty version string")
	}
}

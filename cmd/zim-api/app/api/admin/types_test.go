package admin

import (
	"strings"
	"testing"
)

func TestPermissionCatalogue(t *testing.T) {
	seen := make(map[string]bool)
	for _, item := range Permissions() {
		if !strings.HasPrefix(item.Code, "zim:") || item.Domain == "" {
			t.Fatalf("invalid permission: %+v", item)
		}
		if seen[item.Code] {
			t.Fatalf("duplicate permission: %s", item.Code)
		}
		seen[item.Code] = true
	}
	if len(seen) != 22 {
		t.Fatalf("permission count = %d", len(seen))
	}
}

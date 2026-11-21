// Package conv builds canonical ZIM conversation IDs.
package conv

import (
	"strconv"
	"strings"
)

// Single returns s_<smaller>_<larger>.
func Single(a, b int64) string {
	if a > b {
		a, b = b, a
	}
	return "s_" + strconv.FormatInt(a, 10) + "_" + strconv.FormatInt(b, 10)
}

// Group returns g_<gid>.
func Group(gid int64) string {
	return "g_" + strconv.FormatInt(gid, 10)
}

// ParseSingle returns both uids of an s_<a>_<b> conversation ID.
//
// It lets ephemeral paths (typing) resolve the peer before the first message
// created the conversation row.
func ParseSingle(convID string) (a, b int64, ok bool) {
	if !strings.HasPrefix(convID, "s_") {
		return 0, 0, false
	}
	parts := strings.Split(convID[2:], "_")
	if len(parts) != 2 {
		return 0, 0, false
	}
	a, errA := strconv.ParseInt(parts[0], 10, 64)
	b, errB := strconv.ParseInt(parts[1], 10, 64)
	if errA != nil || errB != nil || a <= 0 || b <= 0 || a == b {
		return 0, 0, false
	}
	return a, b, true
}

// ParseGroup returns the group id of a g_<gid> conversation ID.
func ParseGroup(convID string) (gid int64, ok bool) {
	if !strings.HasPrefix(convID, "g_") {
		return 0, false
	}
	gid, err := strconv.ParseInt(convID[2:], 10, 64)
	if err != nil || gid <= 0 {
		return 0, false
	}
	return gid, true
}

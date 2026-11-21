package message

import (
	"strings"
	"testing"
)

func TestSearchTokens(t *testing.T) {
	tokens := searchTokens("你好 Hello!")
	if len(tokens) == 0 {
		t.Fatal("tokens empty")
	}
	joined := strings.Join(tokens, ",")
	if !strings.Contains(joined, "你好") || !strings.Contains(joined, "he") {
		t.Fatalf("tokens = %v", tokens)
	}
}

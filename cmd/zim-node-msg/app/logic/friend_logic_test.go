package logic

import "testing"

func TestAnswerFriendEvent(t *testing.T) {
	kind, from, to := answerFriendEvent(100, 200, true)
	if kind != 2 || from != 200 || to != 100 {
		t.Fatalf("accept = %d/%d/%d", kind, from, to)
	}
	kind, from, to = answerFriendEvent(100, 200, false)
	if kind != 3 || from != 100 || to != 200 {
		t.Fatalf("reject = %d/%d/%d", kind, from, to)
	}
}

package client

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func TestFromStatusParsesZimCode(t *testing.T) {
	// The server returns errs errors; grpc transports their "%d: message"
	// text in the status message with code Unknown.
	grpcErr := status.Error(codes.Unknown, "180102: seat occupied")
	err := FromStatus(grpcErr)
	if !errs.IsCode(err, 180102) {
		t.Fatalf("err = %v, want code 180102", err)
	}
	if errs.From(err).Message() != "seat occupied" {
		t.Fatalf("message = %q", errs.From(err).Message())
	}
}

func TestFromStatusPassthrough(t *testing.T) {
	if err := FromStatus(nil); err != nil {
		t.Fatalf("nil err = %v", err)
	}
	plain := errors.New("plain error")
	if err := FromStatus(plain); err != plain {
		t.Fatalf("plain error not passed through: %v", err)
	}
	// A status without a ZIM code prefix stays as-is so errs.From wraps it
	// as an internal error.
	noCode := status.Error(codes.NotFound, "not found")
	if err := FromStatus(noCode); err != noCode {
		t.Fatalf("status passthrough = %v", err)
	}
	if !errs.IsCode(FromStatus(status.Error(codes.DeadlineExceeded, "context deadline exceeded")), 900003) {
		t.Fatal("deadline must map to internal error")
	}
}

func TestParseZimCode(t *testing.T) {
	cases := []struct {
		message string
		code    int
		text    string
		ok      bool
	}{
		{"180102: seat occupied", 180102, "seat occupied", true},
		{"0: ok", 0, "", false},
		{"abc: x", 0, "", false},
		{"123: x", 0, "", false},
		{"no colon", 0, "", false},
		{"180100: voice room not exist", 180100, "voice room not exist", true},
	}
	for _, tc := range cases {
		code, text, ok := parseZimCode(tc.message)
		if code != tc.code || text != tc.text || ok != tc.ok {
			t.Fatalf("parseZimCode(%q) = %d, %q, %v; want %d, %q, %v",
				tc.message, code, text, ok, tc.code, tc.text, tc.ok)
		}
	}
}

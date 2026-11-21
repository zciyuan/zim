package upload

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func newService(t *testing.T) *Service {
	t.Helper()
	service, err := New(Config{Dir: t.TempDir(), Secret: "secret", MaxBytes: 1024, TTL: time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return service
}

func TestIssueRejectsInvalidRequests(t *testing.T) {
	service := newService(t)
	cases := []struct {
		name string
		uid  int64
		req  Request
		want error
	}{
		{"no uid", 0, Request{Mime: "image/png", FileSize: 10}, errs.ErrUnauthorized},
		{"bad mime", 1, Request{Mime: "application/pdf", FileSize: 10}, errs.ErrUploadTypeInvalid},
		{"empty size", 1, Request{Mime: "image/png", FileSize: 0}, errs.ErrUploadTooLarge},
		{"too big", 1, Request{Mime: "image/png", FileSize: 2048}, errs.ErrUploadTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := service.Issue(tc.uid, tc.req); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestIssueVerifySaveOpen(t *testing.T) {
	service := newService(t)
	intent, err := service.Issue(42, Request{Mime: "image/png", FileSize: 10})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if intent.ObjectKey == "" || !strings.HasPrefix(intent.ObjectKey, "42/") {
		t.Fatalf("object key = %q", intent.ObjectKey)
	}
	if intent.Method != "PUT" || intent.ExpiresIn != 60 || !strings.HasPrefix(intent.DownloadURL, "/v1/files/") {
		t.Fatalf("intent = %+v", intent)
	}

	// Extract the signature material from the intent and verify it.
	query := intent.UploadURL[strings.Index(intent.UploadURL, "?")+1:]
	values := map[string]string{}
	for _, pair := range strings.Split(query, "&") {
		kv := strings.SplitN(pair, "=", 2)
		values[kv[0]] = kv[1]
	}
	uid := parseInt(t, values["uid"])
	exp := parseInt(t, values["exp"])
	if err := service.Verify(intent.ObjectKey, uid, exp, values["sig"]); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if err := service.Verify(intent.ObjectKey, uid, exp, "deadbeef"); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("bad signature err = %v", err)
	}

	size, err := service.Save(intent.ObjectKey, bytes.NewReader([]byte("png-bytes")))
	if err != nil || size != 9 {
		t.Fatalf("Save = %d, %v", size, err)
	}

	reader, gotSize, _, err := service.Open(intent.ObjectKey)
	if err != nil || gotSize != 9 {
		t.Fatalf("Open = %d, %v", gotSize, err)
	}
	defer func() { _ = reader.Close() }()
	buf := make([]byte, 9)
	if _, err := reader.Read(buf); err != nil || string(buf) != "png-bytes" {
		t.Fatalf("read = %q, %v", buf, err)
	}
}

func TestExpiredSignature(t *testing.T) {
	service := newService(t)
	clock := time.Now()
	service.now = func() time.Time { return clock }
	intent, err := service.Issue(7, Request{Mime: "image/jpeg", FileSize: 5})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	service.now = func() time.Time { return clock.Add(2 * time.Minute) }
	query := intent.UploadURL[strings.Index(intent.UploadURL, "?")+1:]
	var exp int64
	for _, pair := range strings.Split(query, "&") {
		if strings.HasPrefix(pair, "exp=") {
			exp = parseInt(t, strings.TrimPrefix(pair, "exp="))
		}
	}
	if err := service.Verify(intent.ObjectKey, 7, exp, ""); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("expired err = %v", err)
	}
}

func TestSaveRejectsTraversalAndOversize(t *testing.T) {
	service := newService(t)
	bad := []string{"../etc/passwd", "42/202611/../../x.png", "42/202611/short.png", "42/202611/ok.exe"}
	for _, key := range bad {
		if _, err := service.Save(key, bytes.NewReader([]byte("x"))); !errors.Is(err, ErrObjectKeyInvalid) {
			t.Fatalf("Save(%q) err = %v", key, err)
		}
		if _, _, _, err := service.Open(key); !errors.Is(err, ErrObjectKeyInvalid) {
			t.Fatalf("Open(%q) err = %v", key, err)
		}
	}

	intent, err := service.Issue(9, Request{Mime: "image/png", FileSize: 10})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := service.Save(intent.ObjectKey, bytes.NewReader(make([]byte, 4096))); !errors.Is(err, errs.ErrUploadTooLarge) {
		t.Fatalf("oversize err = %v", err)
	}
}

func TestOpenMissingObject(t *testing.T) {
	service := newService(t)
	intent, err := service.Issue(11, Request{Mime: "image/webp", FileSize: 4})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, _, _, err := service.Open(intent.ObjectKey); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(Config{Dir: "", Secret: "s"}); err == nil {
		t.Fatal("empty dir must fail")
	}
	if _, err := New(Config{Dir: t.TempDir(), Secret: ""}); err == nil {
		t.Fatal("empty secret must fail")
	}
}

func parseInt(t *testing.T, value string) int64 {
	t.Helper()
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

func TestIssueAttachmentKinds(t *testing.T) {
	service, err := New(Config{Dir: t.TempDir(), Secret: "secret", TTL: time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cases := []struct {
		name string
		req  Request
		ext  string
	}{
		{"image", Request{FileType: "image", Mime: "image/png", FileSize: 100}, ".png"},
		{"voice", Request{FileType: "voice", Mime: "audio/mpeg", FileSize: 100}, ".mp3"},
		{"voice m4a", Request{FileType: "voice", Mime: "audio/mp4", FileSize: 100}, ".m4a"},
		{"video", Request{FileType: "video", Mime: "video/mp4", FileSize: 100}, ".mp4"},
		{"file pdf", Request{FileType: "file", FileName: "a.pdf", Mime: "application/pdf", FileSize: 100}, ".pdf"},
		{"file unknown", Request{FileType: "file", FileName: "a.exe", FileSize: 100}, ".bin"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			intent, issueErr := service.Issue(7, tc.req)
			if issueErr != nil {
				t.Fatalf("Issue: %v", issueErr)
			}
			if !strings.HasSuffix(intent.ObjectKey, tc.ext) {
				t.Fatalf("key = %s, want suffix %s", intent.ObjectKey, tc.ext)
			}
		})
	}

	if _, err = service.Issue(7, Request{FileType: "voice", Mime: "video/mp4", FileSize: 100}); !errors.Is(err, errs.ErrUploadTypeInvalid) {
		t.Fatalf("voice with video mime err = %v", err)
	}
	if _, err = service.Issue(7, Request{FileType: "video", Mime: "audio/mpeg", FileSize: 100}); !errors.Is(err, errs.ErrUploadTypeInvalid) {
		t.Fatalf("video with audio mime err = %v", err)
	}
	if _, err = service.Issue(7, Request{FileType: "video", Mime: "video/mp4", FileSize: MaxVideoBytes + 1}); !errors.Is(err, errs.ErrUploadTooLarge) {
		t.Fatalf("oversized video err = %v", err)
	}
	if _, err = service.Issue(7, Request{FileType: "file", FileName: "a.zip", FileSize: MaxFileBytes + 1}); !errors.Is(err, errs.ErrUploadTooLarge) {
		t.Fatalf("oversized file err = %v", err)
	}
}

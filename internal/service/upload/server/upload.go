// Package upload 负责签发附件上传意图并保存消息附件。
//
// 客户端契约固定为 token -> presigned PUT -> presigned GET。
// 生产默认使用 SeaweedFS/Ceph RGW 的 S3 API，可切 OSS/S3；
// local provider 仅用于单机开发和测试。
package upload

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

const (
	// MaxImageBytes caps one image upload (5 MiB).
	MaxImageBytes int64 = 5 << 20
	// MaxVoiceBytes caps one voice clip (10 MiB).
	MaxVoiceBytes int64 = 10 << 20
	// MaxVideoBytes caps one video clip (50 MiB).
	MaxVideoBytes int64 = 50 << 20
	// MaxFileBytes caps one generic attachment (20 MiB).
	MaxFileBytes int64 = 20 << 20
	// DefaultTTL is how long an upload intent stays valid.
	DefaultTTL = 15 * time.Minute
	// keyPattern is the only object-key shape this store will touch.
	keyPattern = `^[0-9]+/[0-9]{6}/[a-f0-9]{32}\.` +
		`(png|jpg|jpeg|gif|webp|` + // images
		`mp3|m4a|aac|wav|ogg|` + // voice
		`mp4|mov|webm|` + // video
		`pdf|zip|txt|doc|docx|xls|xlsx|ppt|pptx|csv|bin)$` // files
)

// Message attachment kinds (msg_type 2/3/4/5).
const (
	KindImage = "image"
	KindVoice = "voice"
	KindVideo = "video"
	KindFile  = "file"
)

var (
	// ErrObjectNotFound reports a missing stored object.
	ErrObjectNotFound = dueerrors.New("upload object not found")
	// ErrSignatureInvalid reports a bad or expired upload signature.
	ErrSignatureInvalid = dueerrors.New("upload signature invalid")
	// ErrObjectKeyInvalid reports a key outside the store's namespace.
	ErrObjectKeyInvalid = dueerrors.New("upload object key invalid")

	keyRE = regexp.MustCompile(keyPattern)

	// allowedMime maps an accepted image MIME type to the extension we store it as.
	allowedMime = map[string]string{
		"image/jpeg": ".jpg",
		"image/png":  ".png",
		"image/gif":  ".gif",
		"image/webp": ".webp",
	}

	// voiceMime maps accepted audio MIME types.
	voiceMime = map[string]string{
		"audio/mpeg":  ".mp3",
		"audio/mp3":   ".mp3",
		"audio/mp4":   ".m4a",
		"audio/aac":   ".aac",
		"audio/wav":   ".wav",
		"audio/x-wav": ".wav",
		"audio/ogg":   ".ogg",
		"audio/webm":  ".webm",
	}

	// videoMime maps accepted video MIME types.
	videoMime = map[string]string{
		"video/mp4":       ".mp4",
		"video/quicktime": ".mov",
		"video/webm":      ".webm",
	}

	// fileExts is the attachment extension whitelist.
	fileExts = map[string]struct{}{
		".pdf": {}, ".zip": {}, ".txt": {}, ".doc": {}, ".docx": {},
		".xls": {}, ".xlsx": {}, ".ppt": {}, ".pptx": {}, ".csv": {}, ".bin": {},
	}
)

// Config wires the local store.
type Config struct {
	// Provider is "s3" for SeaweedFS/Ceph RGW/OSS/S3. Empty infers s3 from
	// Endpoint, otherwise local from Dir.
	Provider string
	// Dir is the local storage root; only used by the local provider.
	Dir string
	// Secret signs upload intents (HMAC-SHA256).
	Secret string
	// MaxBytes overrides MaxImageBytes when positive.
	MaxBytes int64
	// TTL overrides DefaultTTL when positive.
	TTL time.Duration
	// S3 settings (SeaweedFS/Ceph RGW/OSS/S3). Endpoint empty uses Dir.
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	// PublicEndpoint is used only for presigned URLs.
	PublicEndpoint string
}

// Request is the POST /v1/upload/token body.
type Request struct {
	FileType string
	FileName string
	FileSize int64
	Mime     string
}

// Intent is what the client needs to perform the upload.
type Intent struct {
	ObjectKey   string
	UploadURL   string
	Method      string
	ExpiresIn   int64
	DownloadURL string
}

// Service issues and validates upload intents.
type Service struct {
	dir      string
	secret   []byte
	maxBytes int64
	ttl      time.Duration
	now      func() time.Time
	objects  *minio.Client
	signer   *minio.Client
	bucket   string
	thumbMu  sync.Mutex
	thumbs   map[string][]byte
}

// New returns a local upload service.
func New(cfg Config) (*Service, error) {
	if cfg.Provider == "s3" && cfg.Endpoint == "" {
		return nil, dueerrors.New("upload s3 endpoint is empty")
	}
	if strings.TrimSpace(cfg.Dir) == "" && cfg.Endpoint == "" {
		return nil, dueerrors.New("upload storage is not configured")
	}
	if cfg.Secret == "" {
		return nil, dueerrors.New("upload secret is empty")
	}
	var objects, signer *minio.Client
	if cfg.Endpoint != "" {
		public := cfg.PublicEndpoint
		if public == "" {
			public = cfg.Endpoint
		}
		secure := cfg.UseSSL
		if strings.HasPrefix(public, "https://") {
			secure = true
		}
		public = strings.TrimPrefix(strings.TrimPrefix(public, "https://"), "http://")
		var signErr error
		signer, signErr = minio.New(public, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Secure: secure, Region: "us-east-1"})
		if signErr != nil {
			return nil, fmt.Errorf("create s3 signer: %w", signErr)
		}
		client, err := minio.New(cfg.Endpoint, &minio.Options{
			Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Region: "us-east-1",
			Secure: cfg.UseSSL,
		})
		if err != nil {
			return nil, fmt.Errorf("create s3 client: %w", err)
		}
		objects = client
		bucket := cfg.Bucket
		if bucket == "" {
			bucket = "zim"
		}
		exists, err := client.BucketExists(context.Background(), bucket)
		if err != nil {
			return nil, fmt.Errorf("check s3 bucket: %w", err)
		}
		if !exists {
			if err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}); err != nil {
				return nil, fmt.Errorf("create s3 bucket: %w", err)
			}
		}
	}
	if cfg.Dir != "" {
		if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
			return nil, fmt.Errorf("create upload dir: %w", err)
		}
	}
	service := &Service{
		dir:      cfg.Dir,
		secret:   []byte(cfg.Secret),
		maxBytes: cfg.MaxBytes,
		ttl:      cfg.TTL,
		now:      time.Now,
		objects:  objects,
		signer:   signer,
		bucket:   cfg.Bucket,
		thumbs:   make(map[string][]byte),
	}
	if service.maxBytes <= 0 {
		service.maxBytes = MaxImageBytes
	}
	if service.ttl <= 0 {
		service.ttl = DefaultTTL
	}
	return service, nil
}

// Issue validates the request and returns a signed upload intent.
func (s *Service) Issue(uid int64, req Request) (*Intent, error) {
	if uid <= 0 {
		return nil, errs.ErrUnauthorized
	}
	kind := normalizeKind(req.FileType)
	ext, err := extensionFor(kind, req.Mime, req.FileName)
	if err != nil {
		return nil, err
	}
	if req.FileSize <= 0 || req.FileSize > s.capFor(kind) {
		return nil, errs.ErrUploadTooLarge
	}

	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	key := fmt.Sprintf("%d/%s/%s%s", uid, s.now().UTC().Format("200601"), hex.EncodeToString(random), ext)
	expires := s.now().Add(s.ttl)
	signature := s.sign(key, uid, expires.Unix())
	uploadURL := fmt.Sprintf("/v1/upload/put?key=%s&uid=%d&exp=%d&sig=%s",
		key, uid, expires.Unix(), signature)
	if s.objects != nil {
		presigned, err := s.signer.PresignedPutObject(context.Background(), s.bucket, key, s.ttl)
		if err != nil {
			return nil, errs.ErrInternal.WithCause(err)
		}
		uploadURL = presigned.String()
	}

	downloadURL := "/v1/files/" + key
	if s.signer != nil {
		presigned, err := s.signer.PresignedGetObject(context.Background(), s.bucket, key, s.ttl, nil)
		if err != nil {
			return nil, errs.ErrInternal.WithCause(err)
		}
		downloadURL = presigned.String()
	}
	return &Intent{
		ObjectKey:   key,
		UploadURL:   uploadURL,
		Method:      "PUT",
		ExpiresIn:   int64(s.ttl.Seconds()),
		DownloadURL: downloadURL,
	}, nil
}

// normalizeKind defaults and validates the attachment kind.
func normalizeKind(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case KindVoice:
		return KindVoice
	case KindVideo:
		return KindVideo
	case KindFile:
		return KindFile
	default:
		return KindImage
	}
}

// capFor returns the size cap of one kind, bounded by the configured ceiling.
func (s *Service) capFor(kind string) int64 {
	limit := MaxImageBytes
	switch kind {
	case KindVoice:
		limit = MaxVoiceBytes
	case KindVideo:
		limit = MaxVideoBytes
	case KindFile:
		limit = MaxFileBytes
	}
	if s.maxBytes > 0 && s.maxBytes < limit {
		return s.maxBytes
	}
	return limit
}

// extensionFor resolves the stored extension for a kind + MIME (+ file name).
func extensionFor(kind, mime, name string) (string, error) {
	mime = strings.ToLower(strings.TrimSpace(strings.Split(mime, ";")[0]))
	switch kind {
	case KindVoice:
		if ext, ok := voiceMime[mime]; ok {
			return ext, nil
		}
		return "", errs.ErrUploadTypeInvalid
	case KindVideo:
		if ext, ok := videoMime[mime]; ok {
			return ext, nil
		}
		return "", errs.ErrUploadTypeInvalid
	case KindFile:
		ext := strings.ToLower(filepath.Ext(strings.TrimSpace(name)))
		// The extension must be one this store accepts; anything else is
		// stored as .bin so the key pattern stays closed.
		if _, ok := fileExts[ext]; !ok {
			ext = ".bin"
		}
		return ext, nil
	default:
		if ext, ok := allowedMime[mime]; ok {
			return ext, nil
		}
		return "", errs.ErrUploadTypeInvalid
	}
}

// Verify checks the signature and expiry of one upload URL.
func (s *Service) Verify(key string, uid, exp int64, signature string) error {
	if !keyRE.MatchString(key) || uid <= 0 {
		return ErrObjectKeyInvalid
	}
	if s.now().After(time.Unix(exp, 0)) {
		return ErrSignatureInvalid
	}
	expected := s.sign(key, uid, exp)
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return ErrSignatureInvalid
	}
	return nil
}

// Save writes the body to the object key, enforcing the size cap.
func (s *Service) Save(key string, body io.Reader) (int64, error) {
	if !keyRE.MatchString(key) {
		return 0, ErrObjectKeyInvalid
	}
	if s.objects != nil {
		data, err := io.ReadAll(io.LimitReader(body, s.maxBytes+1))
		if err != nil {
			return 0, fmt.Errorf("read object: %w", err)
		}
		if int64(len(data)) > s.maxBytes {
			return 0, errs.ErrUploadTooLarge
		}
		_, err = s.objects.PutObject(context.Background(), s.bucket, key,
			bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
		if err != nil {
			return 0, fmt.Errorf("put object: %w", err)
		}
		return int64(len(data)), nil
	}
	full := filepath.Join(s.dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
		return 0, fmt.Errorf("create object dir: %w", err)
	}
	file, err := os.OpenFile(full, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return 0, fmt.Errorf("create object: %w", err)
	}
	defer func() { _ = file.Close() }()

	written, err := io.Copy(file, io.LimitReader(body, s.maxBytes+1))
	if err != nil {
		return 0, fmt.Errorf("write object: %w", err)
	}
	if written > s.maxBytes {
		_ = os.Remove(full)
		return 0, errs.ErrUploadTooLarge
	}
	return written, nil
}

// Open returns the stored object for streaming.
func (s *Service) Open(key string) (io.ReadSeekCloser, int64, time.Time, error) {
	if !keyRE.MatchString(key) {
		return nil, 0, time.Time{}, ErrObjectKeyInvalid
	}
	if s.objects != nil {
		object, err := s.objects.GetObject(context.Background(), s.bucket, key, minio.GetObjectOptions{})
		if err != nil {
			return nil, 0, time.Time{}, ErrObjectNotFound
		}
		info, err := object.Stat()
		if err != nil {
			_ = object.Close()
			return nil, 0, time.Time{}, ErrObjectNotFound
		}
		return object, info.Size, info.LastModified, nil
	}
	full := filepath.Join(s.dir, filepath.FromSlash(key))
	file, err := os.Open(full)
	if err != nil {
		if dueerrors.Is(err, os.ErrNotExist) {
			return nil, 0, time.Time{}, ErrObjectNotFound
		}
		return nil, 0, time.Time{}, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, time.Time{}, err
	}
	return file, info.Size(), info.ModTime(), nil
}

// MaxBytes exposes the configured cap for the REST layer.
func (s *Service) MaxBytes() int64 { return s.maxBytes }

// TTL exposes the configured intent lifetime.
func (s *Service) TTL() time.Duration { return s.ttl }

// sign binds key + uid + expiry into one HMAC.
func (s *Service) sign(key string, uid, exp int64) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(key))
	mac.Write([]byte("|"))
	mac.Write([]byte(strconv.FormatInt(uid, 10)))
	mac.Write([]byte("|"))
	mac.Write([]byte(strconv.FormatInt(exp, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

// Delete removes one object owned by the caller.
func (s *Service) Delete(key string) error {
	if !keyRE.MatchString(key) {
		return ErrObjectKeyInvalid
	}
	if s.objects != nil {
		if err := s.objects.RemoveObject(context.Background(), s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		return nil
	}
	if err := os.Remove(filepath.Join(s.dir, filepath.FromSlash(key))); err != nil {
		if dueerrors.Is(err, os.ErrNotExist) {
			return ErrObjectNotFound
		}
		return err
	}
	return nil
}

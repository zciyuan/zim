// Package upload exposes the attachment upload REST endpoints.
package upload

import (
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/dobyte/due/component/http/v2"
	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	uploadsvc "github.com/zciyuan/ZIM/internal/service/upload/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// NewAPI returns the upload REST module. A nil service answers 900002.
func NewAPI(service *uploadsvc.Service) *API {
	return &API{service: service}
}

// Token implements POST /v1/upload/token (ZIM-02 搂3.4).
func (a *API) Token(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	req := &tokenReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrUploadTypeInvalid)
	}
	intent, err := a.service.Issue(middleware.UID(ctx), uploadsvc.Request{
		FileType: req.FileType,
		FileName: req.FileName,
		FileSize: req.FileSize,
		Mime:     req.Mime,
	})
	if err != nil {
		log.Warnf("upload issue failed: %v cause=%v", err, dueerrors.Cause(err))
		return api.Failure(ctx, err)
	}
	return api.Success(ctx, &tokenRes{
		ObjectKey:   intent.ObjectKey,
		UploadURL:   intent.UploadURL,
		Method:      intent.Method,
		ExpiresIn:   intent.ExpiresIn,
		DownloadURL: intent.DownloadURL,
	})
}

// Put implements PUT /v1/upload/put?key=&uid=&exp=&sig= (the local provider's
// stand-in for an S3 presigned PUT).
func (a *API) Put(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	key := ctx.Query("key")
	uid, uidErr := strconv.ParseInt(ctx.Query("uid"), 10, 64)
	exp, expErr := strconv.ParseInt(ctx.Query("exp"), 10, 64)
	if uidErr != nil || expErr != nil {
		return api.Failure(ctx, errs.ErrUploadTypeInvalid)
	}
	if err := a.service.Verify(key, uid, exp, ctx.Query("sig")); err != nil {
		return api.Failure(ctx, errs.ErrUploadTypeInvalid)
	}
	// A signed key is always bound to the account that requested it.
	if middleware.UID(ctx) != uid {
		return api.Failure(ctx, errs.ErrUnauthorized)
	}

	body := ctx.CTX().Body()
	if int64(len(body)) == 0 {
		return api.Failure(ctx, errs.ErrMessageEmpty)
	}
	size, err := a.service.Save(key, strings.NewReader(string(body)))
	if err != nil {
		return api.Failure(ctx, err)
	}
	return api.Success(ctx, &putRes{OK: true, ObjectKey: key, Size: size})
}

// Complete implements POST /v1/upload/complete. Thumbnail generation is a
// background pipeline in production; the local provider only acknowledges.
func (a *API) Complete(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	req := &completeReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.ObjectKey == "" {
		return api.Failure(ctx, errs.ErrUploadTypeInvalid)
	}
	return api.Success(ctx, &putRes{OK: true, ObjectKey: req.ObjectKey})
}

// File implements GET /v1/files/{key...}: the download URL handed out by Token.
// Object keys are unguessable, so the route is intentionally unauthenticated 鈥?// the same trade-off as a presigned GET, and it keeps <img> usable.
func (a *API) File(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	parsed, parseErr := url.Parse(ctx.CTX().OriginalURL())
	if parseErr != nil {
		return api.Failure(ctx, errs.ErrUploadNotFound)
	}
	key := strings.TrimPrefix(parsed.Path, "/v1/files/")
	thumb := parsed.Query().Get("thumb")
	log.Infof("file request key=%s thumb=%s path=%s url=%s", key, thumb, ctx.CTX().Path(), ctx.CTX().OriginalURL())
	if key == "" {
		return api.Failure(ctx, errs.ErrUploadNotFound)
	}
	if thumb == "1" {
		data, thumbErr := a.service.Thumbnail(key, 320)
		if thumbErr != nil {
			log.Warnf("thumbnail failed: %v", thumbErr)
			return api.Failure(ctx, errs.ErrUploadNotFound)
		}
		fiber := ctx.CTX()
		fiber.Set("Content-Type", "image/jpeg")
		fiber.Set("Content-Length", strconv.Itoa(len(data)))
		fiber.Set("Cache-Control", "public, max-age=86400")
		return fiber.Send(data)
	}
	reader, size, modified, err := a.service.Open(key)
	if err != nil {
		return api.Failure(ctx, errs.ErrUploadNotFound)
	}
	defer func() { _ = reader.Close() }()

	// Objects are capped at MaxBytes, so buffering one image is fine and avoids
	// fiber's stream lifetime pitfalls inside due's handler chain.
	data, err := io.ReadAll(io.LimitReader(reader, a.service.MaxBytes()+1))
	if err != nil {
		return api.Failure(ctx, errs.ErrUploadNotFound)
	}

	fiber := ctx.CTX()
	fiber.Set("Content-Type", contentTypeOf(key))
	fiber.Set("Content-Length", strconv.FormatInt(size, 10))
	fiber.Set("Cache-Control", "public, max-age=31536000, immutable")
	fiber.Set("X-Content-Type-Options", "nosniff")
	if !modified.IsZero() {
		fiber.Set("Last-Modified", modified.UTC().Format(httpTimeFormat))
	}
	return fiber.Send(data)
}

const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

func contentTypeOf(key string) string {
	switch strings.ToLower(path.Ext(key)) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	default:
		return "application/octet-stream"
	}
}

// Delete removes an uploaded object owned by the caller.
func (a *API) Delete(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	key := strings.TrimPrefix(ctx.Params("*"), "/")
	if key == "" || !strings.HasPrefix(key, strconv.FormatInt(middleware.UID(ctx), 10)+"/") {
		return api.Failure(ctx, errs.ErrUploadNotFound)
	}
	if err := a.service.Delete(key); err != nil {
		return api.Failure(ctx, errs.ErrUploadNotFound)
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// DeleteJSON removes an object by key.
func (a *API) DeleteJSON(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	req := &deleteReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.Key == "" {
		return api.Failure(ctx, errs.ErrUploadNotFound)
	}
	if !strings.HasPrefix(req.Key, strconv.FormatInt(middleware.UID(ctx), 10)+"/") {
		return api.Failure(ctx, errs.ErrUploadNotFound)
	}
	if err := a.service.Delete(req.Key); err != nil {
		log.Warnf("delete failed key=%s err=%v", req.Key, err)
		return api.Failure(ctx, errs.ErrUploadNotFound)
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

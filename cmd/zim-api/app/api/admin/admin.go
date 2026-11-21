// Package admin exposes the ZIM admin control-plane API.
package admin

import (
	"context"
	"strconv"

	"github.com/dobyte/due/component/http/v2"
	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	adminsvc "github.com/zciyuan/ZIM/internal/service/admin/server"
	historysvc "github.com/zciyuan/ZIM/internal/service/history/server"
	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	notificationsvc "github.com/zciyuan/ZIM/internal/service/notification/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// API is the admin REST module.
type API struct {
	service       *adminsvc.Service
	history       *historysvc.Service
	notifications *notificationsvc.Service
	kick          pipe.Producer
	cache         redis.UniversalClient
	onConfigWrite func(context.Context)
}

// NewAPI returns the admin API.
func NewAPI(service *adminsvc.Service, history *historysvc.Service,
	notifications *notificationsvc.Service) *API {
	return &API{service: service, history: history, notifications: notifications}
}

// SetKick enables admin forced logout.
func (a *API) SetKick(producer pipe.Producer) { a.kick = producer }

// SetCache enables admin-side cache invalidation.
func (a *API) SetCache(client redis.UniversalClient) { a.cache = client }

// SetConfigInvalidator clears the client-config L1/Redis after admin writes.
func (a *API) SetConfigInvalidator(fn func(context.Context)) { a.onConfigWrite = fn }

// Ping reports admin API liveness.
func (a *API) Ping(ctx http.Context) error {
	return api.Success(ctx, map[string]any{"service": "zim-api", "status": "ok"})
}

// Permissions returns the frozen ZIM admin permission catalogue.
func (a *API) Permissions(ctx http.Context) error {
	return api.Success(ctx, map[string]any{"items": Permissions()})
}

// Users returns a page of users.
func (a *API) Users(ctx http.Context) error {
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	offset, _ := strconv.Atoi(ctx.Query("offset"))
	users, err := a.service.ListUsers(ctx.Context(), limit, offset)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	items := make([]*userItem, 0, len(users))
	for _, user := range users {
		items = append(items, &userItem{
			UID: user.ID, Username: user.Username, Nickname: user.Nickname,
			Avatar: user.FaceURL, Gender: int32(user.Gender), Status: int32(user.Status),
			CreatedAt: user.CreatedAt,
		})
	}
	return api.Success(ctx, map[string]any{"items": items})
}

// BanUser sets a user's status.
func (a *API) BanUser(ctx http.Context) error {
	uid, err := strconv.ParseInt(ctx.Params("uid"), 10, 64)
	if err != nil || uid <= 0 {
		return api.Failure(ctx, errs.ErrUserNotExist)
	}
	req := &banUserReq{}
	if err = ctx.Bind().JSON(req); err != nil {
		log.Warnf("admin ban bind failed: uid=%d err=%v", uid, err)
		return api.Failure(ctx, errs.ErrInternal)
	}
	status := int16(0)
	if req.Banned {
		status = 1
	}
	if err = a.service.SetUserStatus(ctx.Context(), uid, status); err != nil {
		log.Warnf("admin ban failed: uid=%d err=%v", uid, err)
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	if auditErr := a.service.Audit(ctx.Context(), operator(ctx), "user.ban", strconv.FormatInt(uid, 10), requestID(ctx)); auditErr != nil {
		log.Warnf("admin audit failed: uid=%d err=%v", uid, auditErr)
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// Groups returns a page of groups.
func (a *API) Groups(ctx http.Context) error {
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	offset, _ := strconv.Atoi(ctx.Query("offset"))
	groups, err := a.service.ListGroups(ctx.Context(), limit, offset)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	items := make([]*groupItem, 0, len(groups))
	for _, group := range groups {
		items = append(items, &groupItem{
			GID: group.ID, Name: group.Name, Avatar: group.FaceURL,
			OwnerUID: group.OwnerID, GroupType: int32(group.GroupType), CreatedAt: group.CreatedAt,
		})
	}
	return api.Success(ctx, map[string]any{"items": items})
}

// DismissGroup deletes a group.
func (a *API) DismissGroup(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return api.Failure(ctx, errs.ErrGroupNotExist)
	}
	if err = a.service.DismissGroup(ctx.Context(), gid); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	if auditErr := a.service.Audit(ctx.Context(), operator(ctx), "group.dismiss", strconv.FormatInt(gid, 10), requestID(ctx)); auditErr != nil {
		log.Warnf("admin audit failed: gid=%d err=%v", gid, auditErr)
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// Messages returns a conversation message range.
func (a *API) Messages(ctx http.Context) error {
	cid := ctx.Query("conversation_id")
	if cid == "" || a.history == nil {
		return api.Failure(ctx, errs.ErrInternal)
	}
	begin, _ := strconv.ParseInt(ctx.Query("begin_seq"), 10, 64)
	end, _ := strconv.ParseInt(ctx.Query("end_seq"), 10, 64)
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	messages, hasMore, err := a.history.Pull(ctx.Context(), cid, begin, end, limit)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	items := make([]*messageItem, 0, len(messages))
	for _, message := range messages {
		items = append(items, &messageItem{
			ServerMsgID: message.ServerMsgID, ClientMsgID: message.ClientMsgID,
			ConversationID: message.ConversationID, Seq: message.Seq,
			SendID: message.SendID, RecvID: message.RecvID, GroupID: message.GroupID,
			MsgType: message.MsgType, Content: message.Content, SendTime: message.SendTime,
		})
	}
	return api.Success(ctx, map[string]any{"items": items, "has_more": hasMore})
}

// Audit returns recent admin audit records.
func (a *API) Audit(ctx http.Context) error {
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	items, err := a.service.ListAudit(ctx.Context(), ctx.Query("operator"), limit)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"items": items})
}

func operator(ctx http.Context) string {
	if value := ctx.Get("X-ZIM-Admin-Operator"); value != "" {
		return value
	}
	return "unknown"
}

func requestID(ctx http.Context) string { return ctx.Get("X-Request-Id") }

// Stats returns dashboard counts.
func (a *API) Stats(ctx http.Context) error {
	stats, err := a.service.Stats(ctx.Context())
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, stats)
}

// Families returns family groups.
func (a *API) Families(ctx http.Context) error {
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	offset, _ := strconv.Atoi(ctx.Query("offset"))
	groups, err := a.service.ListFamilies(ctx.Context(), limit, offset)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	items := make([]*groupItem, 0, len(groups))
	for _, group := range groups {
		items = append(items, &groupItem{
			GID: group.ID, Name: group.Name, Avatar: group.FaceURL,
			OwnerUID: group.OwnerID, GroupType: int32(group.GroupType), CreatedAt: group.CreatedAt,
		})
	}
	return api.Success(ctx, map[string]any{"items": items})
}

// GetConfig returns one admin setting.
func (a *API) GetConfig(ctx http.Context) error {
	item, err := a.service.GetSetting(ctx.Context(), ctx.Params("key"))
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, item)
}

// SetConfig writes one admin setting.
func (a *API) SetConfig(ctx http.Context) error {
	req := &settingReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if err := a.service.SetSetting(ctx.Context(), ctx.Params("key"), req.Value); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	if a.cache != nil {
		_ = a.cache.Del(ctx.Context(), "zim:clientconfig:all").Err()
	}
	if a.onConfigWrite != nil {
		a.onConfigWrite(ctx.Context())
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// SensitiveWords lists blocked words.
func (a *API) SensitiveWords(ctx http.Context) error {
	items, err := a.service.ListSensitiveWords(ctx.Context())
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"items": items})
}

// AddSensitiveWord adds a blocked word.
func (a *API) AddSensitiveWord(ctx http.Context) error {
	req := &sensitiveWordReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.Word == "" {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if err := a.service.AddSensitiveWord(ctx.Context(), req.Word, req.Action); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	if a.cache != nil {
		_ = a.cache.Incr(ctx.Context(), "zim:sensitive:version").Err()
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// DeleteSensitiveWord removes a blocked word.
func (a *API) DeleteSensitiveWord(ctx http.Context) error {
	id, err := strconv.ParseInt(ctx.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if err = a.service.DeleteSensitiveWord(ctx.Context(), id); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	if a.cache != nil {
		_ = a.cache.Incr(ctx.Context(), "zim:sensitive:version").Err()
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// Reports lists abuse reports.
func (a *API) Reports(ctx http.Context) error {
	status := int16(-1)
	if value := ctx.Query("status"); value != "" {
		parsed, _ := strconv.ParseInt(value, 10, 16)
		status = int16(parsed)
	}
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	items, err := a.service.ListReports(ctx.Context(), status, limit)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"items": items})
}

// CreateReport records an abuse report.
func (a *API) CreateReport(ctx http.Context) error {
	req := &reportReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.TargetID == "" {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if err := a.service.CreateReport(ctx.Context(), req.ReporterID, req.TargetType, req.TargetID, req.Reason); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// HandleReport updates report status.
func (a *API) HandleReport(ctx http.Context) error {
	id, err := strconv.ParseInt(ctx.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return api.Failure(ctx, errs.ErrInternal)
	}
	req := &handleReportReq{}
	if err = ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if err = a.service.HandleReport(ctx.Context(), id, req.Status); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// PushCerts lists configured push platforms.
func (a *API) PushCerts(ctx http.Context) error {
	items, err := a.service.ListPushCerts(ctx.Context())
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"items": items})
}

// SetPushCert writes one platform certificate.
func (a *API) SetPushCert(ctx http.Context) error {
	platform, err := strconv.ParseInt(ctx.Params("platform"), 10, 16)
	if err != nil || platform <= 0 {
		return api.Failure(ctx, errs.ErrInternal)
	}
	req := &pushCertReq{}
	if err = ctx.Bind().JSON(req); err != nil || req.Cert == "" {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if err = a.service.SetPushCert(ctx.Context(), int16(platform), req.Cert); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// Ops returns an operations overview.
func (a *API) Ops(ctx http.Context) error {
	stats, err := a.service.Stats(ctx.Context())
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	certs, _ := a.service.ListPushCerts(ctx.Context())
	return api.Success(ctx, map[string]any{"stats": stats, "push_certs": certs})
}

// Notifications lists announcements for the admin console.
func (a *API) Notifications(ctx http.Context) error {
	if a.notifications == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	items, err := a.notifications.List(ctx.Context(), 0, 100)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"items": items})
}

// PublishNotification creates an announcement.
func (a *API) PublishNotification(ctx http.Context) error {
	if a.notifications == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	req := &notificationReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.Title == "" {
		return api.Failure(ctx, errs.ErrInternal)
	}
	id, err := a.notifications.Publish(ctx.Context(), req.Title, req.Content, 0)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"id": id})
}

// WebhookDLQ lists failed webhook events.
func (a *API) WebhookDLQ(ctx http.Context) error {
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	items, err := a.service.ListWebhookDLQ(ctx.Context(), limit)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"items": items})
}

// DeleteWebhookDLQ removes one failed event.
func (a *API) DeleteWebhookDLQ(ctx http.Context) error {
	id, err := strconv.ParseInt(ctx.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if err = a.service.DeleteWebhookDLQ(ctx.Context(), id); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// SetGroupSearchable toggles group search visibility.
func (a *API) SetGroupSearchable(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return api.Failure(ctx, errs.ErrGroupNotExist)
	}
	req := &searchableReq{}
	if err = ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if err = a.service.SetGroupSearchable(ctx.Context(), gid, req.Searchable); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// Kick closes all active sessions of a user.
func (a *API) Kick(ctx http.Context) error {
	uid, err := strconv.ParseInt(ctx.Params("uid"), 10, 64)
	if err != nil || uid <= 0 {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if a.kick == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	event := &messagesvc.Event{Type: "admin.kick", Recipients: []int64{uid}, Content: "2"}
	value, err := json.Marshal(event)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	if err = a.kick.Publish(ctx.Context(), pipe.Message{
		Topic: "zim.pipe.toPush", Key: strconv.FormatInt(uid, 10), Value: value,
	}); err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

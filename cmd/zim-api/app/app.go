// Package app assembles the HTTP API application.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/dobyte/due/component/http/v2"
	"github.com/dobyte/due/v2/etc"

	api "github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	adminapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/admin"
	authapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/auth"
	clientconfigapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/clientconfig"
	convapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/conversation"
	groupapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/group"
	notificationapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/notification"
	relationapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/relation"
	syncapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/sync"
	uploadapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/upload"
	userapi "github.com/zciyuan/ZIM/cmd/zim-api/app/api/user"
	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	"github.com/zciyuan/ZIM/internal/component/token"
	adminsvc "github.com/zciyuan/ZIM/internal/service/admin/server"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	clientconfigserver "github.com/zciyuan/ZIM/internal/service/clientconfig/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	groupreadsvc "github.com/zciyuan/ZIM/internal/service/groupread/server"
	groupversionsvc "github.com/zciyuan/ZIM/internal/service/groupversion/server"
	historysvc "github.com/zciyuan/ZIM/internal/service/history/server"
	notificationserver "github.com/zciyuan/ZIM/internal/service/notification/server"
	presenceserver "github.com/zciyuan/ZIM/internal/service/presence/server"
	privacyserver "github.com/zciyuan/ZIM/internal/service/privacy/server"
	rtcsvc "github.com/zciyuan/ZIM/internal/service/rtc/server"
	uploadsvc "github.com/zciyuan/ZIM/internal/service/upload/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/pipe"
	rtcfw "github.com/zciyuan/ZIM/zimfw/rtc"
)

// Init registers the REST routes.
func Init(proxy *http.Proxy, issuer *token.Issuer,
	conversationService *conversationsvc.Service, historyService *historysvc.Service,
	syncService *changelogsvc.Service, groupRead *groupreadsvc.Service, families *familysvc.Service,
	adminService *adminsvc.Service, uploadService *uploadsvc.Service, cache *rediscomp.Redis,
	clientConfig *clientconfigserver.Service, groupVersions *groupversionsvc.Service,
	notifications *notificationserver.Service, privacy *privacyserver.Service, presence *presenceserver.Service, kick pipe.Producer) {
	// 认证模块
	{
		auth := authapi.NewAPI(proxy)
		group := proxy.Router().Group("/v1/auth")
		group.Post("/token", auth.Token)
		group.Post("/register", auth.Register)
		group.Post("/refresh", auth.Refresh)
	}

	user := userapi.NewAPI(proxy)
	user.SetPrivacy(privacy)
	user.SetPresence(presence)
	relation := relationapi.NewAPI(proxy)
	group := groupapi.NewAPI(proxy, groupRead, families)
	group.SetGroupVersions(groupVersions)
	conv := convapi.NewAPI(conversationService, historyService)
	syncAPI := syncapi.NewAPI(syncService)
	clientConfigAPI := clientconfigapi.NewAPI(clientConfig)
	notificationAPI := notificationapi.NewAPI(notifications)

	// 公共配置
	{
		group := proxy.Router().Group("/v1")
		group.Get("/client-config", clientConfigAPI.Get)
		group.Get("/notifications", notificationAPI.List)

		// LiveKit webhook is public but authenticated by the LiveKit signature.
		rtcService := rtcsvc.New(cache.Client())
		rtcURL := etc.Get("etc.zim.rtc.url", "").String()
		var deleteRoom func(context.Context, string) error
		if rtcURL != "" {
			provider := rtcfw.NewLiveKit(rtcfw.LiveKitConfig{
				URL: rtcURL, APIKey: etc.Get("etc.zim.rtc.apiKey", "").String(),
				APISecret: etc.Get("etc.zim.rtc.apiSecret", "").String(),
			})
			deleteRoom = provider.DeleteRoom
		}
		group.Post("/rtc/livekit/webhook", func(ctx http.Context) error {
			body, err := io.ReadAll(ctx.StdRequest().Body)
			if err != nil {
				return api.Failure(ctx, errs.ErrInternal.WithCause(err))
			}
			if err = rtcService.HandleWebhook(ctx.Context(), body, ctx.Get("Authorization"),
				etc.Get("etc.zim.rtc.apiSecret", "").String(), deleteRoom); err != nil {
				return api.Failure(ctx, errs.ErrUnauthorized)
			}
			return ctx.SendStatus(204)
		})
	}

	// 登录后模块统一挂鉴权中间件
	secured := proxy.Router().Group("/v1", middleware.Auth(issuer))
	secured.Post("/notifications/:id/read", notificationAPI.MarkRead)
	secured.Get("/notifications/read", notificationAPI.ReadIDs)
	secured.Get("/ws/ticket", func(ctx http.Context) error {
		uid := middleware.UID(ctx)
		platform := int64(2)
		if value := ctx.Query("platform"); value != "" {
			if parsed, parseErr := strconv.ParseInt(value, 10, 32); parseErr == nil && parsed > 0 {
				platform = parsed
			}
		}
		raw := make([]byte, 16)
		if _, randErr := rand.Read(raw); randErr != nil {
			return api.Failure(ctx, errs.ErrInternal.WithCause(randErr))
		}
		ticket := "t_" + hex.EncodeToString(raw)
		if setErr := cache.Client().Set(ctx.Context(), "zim:ws:ticket:"+ticket,
			fmt.Sprintf("%d:%d", uid, platform), time.Minute).Err(); setErr != nil {
			return api.Failure(ctx, errs.ErrInternal.WithCause(setErr))
		}
		transports := make([]map[string]any, 0, 3)
		if url := etc.Get("etc.zim.transport.wsUrl", "").String(); url != "" {
			transports = append(transports, map[string]any{"type": "ws", "url": url})
		}
		if addr := etc.Get("etc.zim.transport.quicAddr", "").String(); addr != "" {
			transports = append(transports, map[string]any{"type": "quic", "addr": addr, "alpn": "due-quic"})
		}
		if url := etc.Get("etc.zim.transport.webtransportUrl", "").String(); url != "" {
			transports = append(transports, map[string]any{"type": "webtransport", "url": url})
		}
		return api.Success(ctx, map[string]any{"ticket": ticket, "expires_in": 60, "transports": transports})
	})

	admin := adminapi.NewAPI(adminService, historyService, notifications)
	admin.SetCache(cache.Client())
	admin.SetConfigInvalidator(clientConfig.Invalidate)
	admin.SetKick(kick)
	adminGroup := proxy.Router().Group("/admin", middleware.Admin(etc.Get("etc.zim.admin.token").String(), etc.Get("etc.zim.admin.source").String()))
	adminGroup.Get("/ping", admin.Ping)
	adminGroup.Get("/permissions", admin.Permissions)
	adminGroup.Get("/users", middleware.Require("zim:user:list"), admin.Users)
	adminGroup.Post("/users/:uid/ban", middleware.Require("zim:user:ban"), admin.BanUser)
	adminGroup.Post("/users/:uid/kick", middleware.Require("zim:user:kick"), admin.Kick)
	adminGroup.Get("/groups", middleware.Require("zim:group:list"), admin.Groups)
	adminGroup.Post("/groups/:gid/dismiss", middleware.Require("zim:group:dismiss"), admin.DismissGroup)
	adminGroup.Post("/groups/:gid/searchable", middleware.Require("zim:group:dismiss"), admin.SetGroupSearchable)
	adminGroup.Get("/messages", middleware.Require("zim:msg:query"), admin.Messages)
	adminGroup.Get("/audit", middleware.Require("zim:oplog:list"), admin.Audit)
	adminGroup.Get("/stats", admin.Stats)
	adminGroup.Get("/notifications", admin.Notifications)
	adminGroup.Get("/webhook/dlq", middleware.Require("zim:oplog:list"), admin.WebhookDLQ)
	adminGroup.Delete("/webhook/dlq/:id", middleware.Require("zim:config:write"), admin.DeleteWebhookDLQ)
	adminGroup.Post("/notifications", admin.PublishNotification)
	adminGroup.Get("/families", middleware.Require("zim:family:list"), admin.Families)
	adminGroup.Post("/families/:gid/dismiss", middleware.Require("zim:family:dismiss"), admin.DismissGroup)
	adminGroup.Get("/config/:key", middleware.Require("zim:config:read"), admin.GetConfig)
	adminGroup.Put("/config/:key", middleware.Require("zim:config:write"), admin.SetConfig)
	adminGroup.Get("/sensitive-words", middleware.Require("zim:sensitive:list"), admin.SensitiveWords)
	adminGroup.Post("/sensitive-words", middleware.Require("zim:sensitive:write"), admin.AddSensitiveWord)
	adminGroup.Delete("/sensitive-words/:id", middleware.Require("zim:sensitive:write"), admin.DeleteSensitiveWord)
	adminGroup.Get("/reports", middleware.Require("zim:audit:report"), admin.Reports)
	adminGroup.Post("/reports", middleware.Require("zim:audit:report"), admin.CreateReport)
	adminGroup.Post("/reports/:id/handle", middleware.Require("zim:audit:content"), admin.HandleReport)
	adminGroup.Get("/push/certs", middleware.Require("zim:push:cert"), admin.PushCerts)
	adminGroup.Put("/push/certs/:platform", middleware.Require("zim:push:cert"), admin.SetPushCert)
	adminGroup.Get("/ops", admin.Ops)

	secured.Get("/users/me", user.Me)
	secured.Get("/users/me/privacy", user.Privacy)
	secured.Put("/users/me/privacy", user.UpdatePrivacy)
	secured.Put("/users/me", user.UpdateMe)
	secured.Get("/users/search", user.Search)
	secured.Get("/users/presence", user.Presence)
	secured.Get("/users/:uid", user.Get)
	secured.Get("/users", user.Batch)

	secured.Get("/friends", relation.Friends)
	secured.Post("/friends", relation.AddFriend)
	secured.Delete("/friends/:uid", relation.DeleteFriend)
	secured.Put("/friends/:uid/remark", relation.UpdateRemark)
	secured.Post("/friends/:uid/pin", relation.PinFriend)
	secured.Get("/friend-requests", relation.Requests)
	secured.Post("/friend-requests/answer", relation.AnswerFriend)
	secured.Get("/blacklist", relation.Blacklist)
	secured.Post("/blacklist", relation.AddBlack)
	secured.Delete("/blacklist/:uid", relation.RemoveBlack)
	secured.Get("/sync/changes", syncAPI.Changes)

	secured.Get("/conversations", conv.List)
	secured.Get("/conversations/active", conv.Active)
	secured.Get("/conversations/:cid/messages", conv.Messages)
	secured.Get("/conversations/:cid/search", conv.Search)
	secured.Get("/conversations/:cid/unread", conv.Unread)
	secured.Post("/conversations/:cid/read", conv.Read)
	secured.Post("/conversations/:cid/pin", conv.Pin)
	secured.Delete("/conversations/:cid", conv.Delete)
	secured.Post("/conversations/:cid/clear", conv.Clear)

	// Attachments (ZIM-02 §3.4): token -> presigned PUT -> immutable GET.
	upload := uploadapi.NewAPI(uploadService)
	secured.Post("/upload/token", upload.Token)
	secured.Put("/upload/put", upload.Put)
	secured.Post("/upload/complete", upload.Complete)
	proxy.Router().Get("/v1/files/*", upload.File)
	proxy.Router().Post("/v1/upload/delete", middleware.Auth(issuer), upload.DeleteJSON)
	proxy.Router().Delete("/v1/files/*", middleware.Auth(issuer), upload.Delete)

	secured.Get("/groups", group.MyGroups)
	secured.Get("/groups/:gid", group.Get)
	secured.Get("/groups/:gid/members", group.Members)
	secured.Get("/groups/:gid/read", group.ReadCount)
	secured.Get("/groups/:gid/versions", group.Versions)
	secured.Get("/groups/:gid/applies", group.Applies)
	secured.Post("/groups/:gid/applies", group.Apply)
	secured.Post("/groups/:gid/applies/answer", group.Answer)

	secured.Post("/family", group.CreateFamily)
	secured.Get("/family/:gid", group.Get)
	secured.Delete("/family/:gid", group.DismissFamily)
	secured.Post("/family/:gid/members", group.InviteFamilyMembers)
	secured.Delete("/family/:gid/members/:uid", group.RemoveFamilyMember)
	secured.Put("/family/:gid/settings", group.UpdateFamilySettings)
	secured.Post("/family/:gid/members/:uid/consent", group.GrantFamilyConsent)
	secured.Get("/family/:gid/audit", group.FamilyAudit)
}

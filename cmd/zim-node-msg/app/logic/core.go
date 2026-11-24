package logic

import (
	"context"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/component/ratelimit"
	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	authsvc "github.com/zciyuan/ZIM/internal/service/auth/client"
	authpb "github.com/zciyuan/ZIM/internal/service/auth/pb"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	devicesvc "github.com/zciyuan/ZIM/internal/service/device/server"
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	groupreadsvc "github.com/zciyuan/ZIM/internal/service/groupread/server"
	message "github.com/zciyuan/ZIM/internal/service/message/server"
	reactionsvc "github.com/zciyuan/ZIM/internal/service/reaction/server"
	rtcsvc "github.com/zciyuan/ZIM/internal/service/rtc/server"
	syncsvc "github.com/zciyuan/ZIM/internal/service/syncsvc/server"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
	"github.com/zciyuan/ZIM/zimfw/route"
	rtcfw "github.com/zciyuan/ZIM/zimfw/rtc"
)

const authCallTimeout = 3 * time.Second

// disconnectGrace lets a reconnecting client re-bind the same device key before
// the old connection's disconnect prunes it.
const disconnectGrace = 1500 * time.Millisecond

// loginContext is the subset of node.Context used by the login use case. It
// keeps the parser/response seam small enough to test without a live node.
type loginContext interface {
	Parse(v any) error
	Response(v any) error
	BindGate(uid ...int64) error
	Context() context.Context
	CID() int64
	GID() string
	Route() int32
}

// core is the message node application core. It owns route registration and
// the login use case; it has no process or transport startup logic.

// NewCore returns the message node core.
func NewCore(proxy *node.Proxy, msg *message.Service, pipeline *message.Pipeline,
	conv *conversationsvc.Service, changes *changelogsvc.Service,
	groupRead *groupreadsvc.Service, rtcCalls *rtcsvc.Service, rtcProvider rtcfw.Provider,
	families *familysvc.Service) *core {
	c := &core{proxy: proxy, msg: msg, pipeline: pipeline, conv: conv, changes: changes,
		groupRead: groupRead, rtcCalls: rtcCalls, rtcProvider: rtcProvider, family: families,
		watches:     make(map[int64]*presenceWatch),
		binds:       newKeyedActor(proxy, "zim:bind"),
		rtcActors:   newKeyedActor(proxy, "zim:rtc"),
		sendLimiter: ratelimit.New(etc.Get("etc.zim.msg.sendRate", 200).Int(), etc.Get("etc.zim.msg.sendBurst", 400).Int())}
	c.rtcClientURL = "wss://rtc.local"
	c.verify = c.verifyToken
	c.bind = c.bindDevice
	return c
}

// WithConnRegistry wires the reverse connection index used by ADR-0022.
func (c *core) WithConnRegistry(registry route.ConnRegistry) *core {
	c.conns = registry
	return c
}

// WithSync wires the multi-device sync service used by ADR-0023.
func (c *core) WithSync(service *syncsvc.Service) *core {
	c.sync = service
	return c
}

// WithDevices wires the device registry used by ADR-0024.
func (c *core) WithDevices(service *devicesvc.Service) *core {
	c.devices = service
	return c
}

// WithDeviceIndex wires the online device index (Redis udev sets) used to pick
// push targets. Falls back to the device table when unset.
func (c *core) WithDeviceIndex(index route.DeviceSource) *core {
	c.deviceIndex = index
	return c
}

// Init registers the message node routes.
func (c *core) Init() {
	go c.runPresenceWatch(context.Background())
	c.proxy.Router().AddRouteHandler(contract.OpLogin, c.login)
	c.proxy.Router().AddRouteHandler(contract.OpLogout, c.logout)
	c.proxy.Router().AddRouteHandler(contract.OpSendSingle, c.sendSingle)
	c.proxy.Router().AddRouteHandler(contract.OpSendGroup, c.sendGroup)
	c.proxy.Router().AddRouteHandler(contract.OpPullBySeq, c.pullBySeq)
	c.proxy.Router().AddRouteHandler(contract.OpReadReceipt, c.readReceipt)
	c.proxy.Router().AddRouteHandler(contract.OpSyncFriend, c.syncFriend)
	c.proxy.Router().AddRouteHandler(contract.OpFullSync, c.fullSync)
	c.proxy.Router().AddRouteHandler(contract.OpSyncGroupMember, c.syncGroupMember)
	c.proxy.Router().AddRouteHandler(contract.OpSyncCheck, c.syncCheck)
	c.proxy.Router().AddRouteHandler(contract.OpSyncMissing, c.syncMissing)
	c.proxy.Router().AddRouteHandler(contract.OpCreateGroup, c.createGroup)
	c.proxy.Router().AddRouteHandler(contract.OpDismissGroup, c.dismissGroup)
	c.proxy.Router().AddRouteHandler(contract.OpQuitGroup, c.quitGroup)
	c.proxy.Router().AddRouteHandler(contract.OpInviteGroup, c.inviteGroupMembers)
	c.proxy.Router().AddRouteHandler(contract.OpApplyJoinGroup, c.applyJoinGroup)
	c.proxy.Router().AddRouteHandler(contract.OpAnswerJoin, c.answerJoinGroup)
	c.proxy.Router().AddRouteHandler(contract.OpKickGroup, c.kickGroupMember)
	c.proxy.Router().AddRouteHandler(contract.OpTransferGroup, c.transferGroup)
	c.proxy.Router().AddRouteHandler(contract.OpUpdateGroup, c.updateGroup)
	c.proxy.Router().AddRouteHandler(contract.OpGroupMembers, c.groupMembers)
	c.proxy.Router().AddRouteHandler(contract.OpSetGroupAdmin, c.setGroupAdmin)
	c.proxy.Router().AddRouteHandler(contract.OpMuteMember, c.muteGroupMember)
	c.proxy.Router().AddRouteHandler(contract.OpMuteAll, c.muteAllGroup)
	c.proxy.Router().AddRouteHandler(contract.OpMyGroups, c.myGroups)
	c.proxy.Router().AddRouteHandler(contract.OpSearchGroup, c.searchGroups)
	c.proxy.Router().AddRouteHandler(contract.OpGroupNotify, c.groupNotify)
	c.proxy.Router().AddRouteHandler(contract.OpCallInvite, c.callInvite)
	c.proxy.Router().AddRouteHandler(contract.OpCallAnswer, c.callAnswer)
	c.proxy.Router().AddRouteHandler(contract.OpCallHangup, c.callHangup)
	c.proxy.Router().AddRouteHandler(contract.OpCallCancel, c.callCancel)
	c.proxy.Router().AddRouteHandler(contract.OpRoomToken, c.roomToken)
	c.proxy.Router().AddRouteHandler(contract.OpCallState, c.callState)
	c.proxy.Router().AddRouteHandler(contract.OpParticipantState, c.participantState)
	c.proxy.Router().AddRouteHandler(contract.OpRecall, c.recall)
	c.proxy.Router().AddRouteHandler(contract.OpQuote, c.quoteMessage)
	c.proxy.Router().AddRouteHandler(contract.OpMerge, c.mergeMessage)
	c.proxy.Router().AddRouteHandler(contract.OpTyping, c.typing)
	c.proxy.Router().AddRouteHandler(contract.OpSubscribeOnline, c.subscribeOnline)
	c.proxy.Router().AddRouteHandler(contract.OpAddFriend, c.addFriend)
	c.proxy.Router().AddRouteHandler(contract.OpAnswerFriend, c.answerFriend)
	c.proxy.Router().AddRouteHandler(contract.OpDeleteFriend, c.deleteFriend)
	c.proxy.Router().AddRouteHandler(contract.OpAddBlack, c.addBlack)
	c.proxy.Router().AddRouteHandler(contract.OpEdit, c.editMessage)
	c.proxy.Router().AddRouteHandler(contract.OpDeleteMsg, c.deleteMessage)
	c.proxy.Router().AddRouteHandler(contract.OpSearchMsg, c.searchMessage)
	c.proxy.Router().AddRouteHandler(contract.OpAddReaction, c.addReaction)
	c.proxy.Router().AddRouteHandler(contract.OpRemoveReaction, c.removeReaction)
	// ADR-0043: burn after reading, screenshot report, group security.
	c.proxy.Router().AddRouteHandler(contract.OpBurnReadAck, c.burnReadAck)
	c.proxy.Router().AddRouteHandler(contract.OpScreenshotReport, c.screenshotReport)
	c.proxy.Router().AddRouteHandler(contract.OpSetGroupSecurity, c.setGroupSecurity)
	c.proxy.Router().AddRouteHandler(contract.OpSetMemberRestriction, c.setMemberRestriction)
	// ADR-0043: voice room lifecycle and seat control (forward to zim-voiceroom).
	c.proxy.Router().AddRouteHandler(contract.OpVoiceRoomCreate, c.voiceRoomCreate)
	c.proxy.Router().AddRouteHandler(contract.OpVoiceRoomList, c.voiceRoomList)
	c.proxy.Router().AddRouteHandler(contract.OpVoiceRoomJoin, c.voiceRoomJoin)
	c.proxy.Router().AddRouteHandler(contract.OpVoiceRoomRefreshToken, c.voiceRoomRefreshToken)
	c.proxy.Router().AddRouteHandler(contract.OpTakeSeat, c.takeSeat)
	c.proxy.Router().AddRouteHandler(contract.OpLeaveSeat, c.leaveSeat)
	c.proxy.Router().AddRouteHandler(contract.OpKickSeat, c.kickSeat)
	c.proxy.Router().AddRouteHandler(contract.OpForceMuteSeat, c.forceMuteSeat)
	c.proxy.Router().AddRouteHandler(contract.OpLockSeat, c.lockSeat)
	c.proxy.Router().AddRouteHandler(contract.OpHandUp, c.handUp)
	c.proxy.Router().AddRouteHandler(contract.OpApproveHandUp, c.approveHandUp)
	c.proxy.Router().AddRouteHandler(contract.OpInviteToSeat, c.inviteToSeat)

	// Connect/reconnect are delivered to every node in the gate's dispatcher.
	// Register no-op handlers so an anonymous connection does not produce a
	// per-connection "not found event" warning (20k reconnects = 40k log lines).
	c.proxy.AddEventHandler(cluster.Connect, func(node.Context) {})
	c.proxy.AddEventHandler(cluster.Reconnect, func(node.Context) {})

	// Connection close must clear the device binding (presence + push targets).
	c.proxy.AddEventHandler(cluster.Disconnect, func(ctx node.Context) {
		c.onDisconnect(ctx)
	})
}

// onDisconnect prunes the device binding of a closed connection so presence
// turns false (and pushes stop targeting it) without waiting for a logout.
//
// The prune is delayed: a reconnecting client binds the same device key within
// a few hundred milliseconds, and removing a key that a live connection just
// claimed silently kills every push to that device (no read receipts, no
// typing, no calls) until the next login.
func (c *core) onDisconnect(ctx node.Context) {
	uid, platform, slot, err := multi.ParseSlot(ctx.UID())
	if err != nil {
		return
	}
	gid := ctx.GID()
	go func() {
		time.Sleep(disconnectGrace)
		router, routerErr := route.New(c.proxy, gid, route.WithConnRegistry(c.conns))
		if routerErr != nil {
			return
		}
		// A reconnect binds the same device key before the old connection's
		// disconnect is processed: only prune when nobody owns the key any more.
		locateCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		current, locateErr := router.Locate(locateCtx, uid, platform, slot)
		cancel()
		if locateErr == nil && current != "" {
			return
		}
		unbindCtx, unbindCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer unbindCancel()
		unbindErr := c.binds.Do(unbindCtx, strconv.FormatInt(uid, 10), func() error {
			return router.Unbind(unbindCtx, uid, platform, slot)
		})
		if unbindErr != nil {
			log.Warnf("disconnect unbind failed: uid = %d, platform = %d, slot = %d, err = %v",
				uid, platform, slot, unbindErr)
		}
		// A dropped connection is a leave for group/family rooms; the last
		// participant ends the call and deletes the SFU room.
		c.leaveCallOnDisconnect(context.Background(), uid)
	}()
}

// logout handles op:1002. The device binding is dropped before replying so that
// presence (which reads the per-user online device set) turns false immediately
// and later pushes stop targeting a dead device key.
func (c *core) logout(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, platform, slot, parseErr := multi.ParseSlot(ctx.UID())
		if parseErr == nil {
			if router, routerErr := route.New(c.proxy, ctx.GID(), route.WithConnRegistry(c.conns)); routerErr == nil {
				unbindErr := c.binds.Do(ctx.Context(), strconv.FormatInt(uid, 10), func() error {
					return router.Unbind(ctx.Context(), uid, platform, slot)
				})
				if unbindErr != nil {
					log.Warnf("logout unbind failed: uid = %d, platform = %d, slot = %d, err = %v",
						uid, platform, slot, unbindErr)
				}
			}
			// Logout retires the device row; a later login reuses the same slot
			// and registers it again.
			if c.devices != nil {
				if deactivateErr := c.devices.DeactivateSlot(ctx.Context(), uid, int16(platform), int16(slot)); deactivateErr != nil {
					log.Warnf("logout deactivate device failed: uid = %d, platform = %d, slot = %d, err = %v",
						uid, platform, slot, deactivateErr)
				}
			}
		}
		if err := ctx.UnbindGate(); err != nil {
			c.respond(ctx, contract.OpLogout, nil, errs.ErrInternal.WithCause(err))
			return
		}
		c.respond(ctx, contract.OpLogout, nil, nil)
	})
}

func (c *core) login(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		if err := c.doLogin(ctx); err != nil {
			log.Errorf("login failed: cid = %d, route = %d, err = %v", ctx.CID(), ctx.Route(), err)
		}
	})
}

func (c *core) doLogin(ctx loginContext) error {
	envelope := &contract.Envelope{}
	if err := ctx.Parse(envelope); err != nil {
		c.respond(ctx, contract.OpLogin, nil, errs.ErrInternal.WithCause(err))
		return err
	}

	req := &authwire.LoginReq{}
	if err := proto.Unmarshal(envelope.GetPayload(), req); err != nil {
		c.respond(ctx, contract.OpLogin, nil, errs.ErrInternal.WithCause(err))
		return err
	}

	service := newLoginService(c.verify, func(bindCtx context.Context, uid int64, platform int32, slot int32) error {
		return c.bind(bindCtx, ctx, uid, platform, slot)
	})
	service.slot = func(slotCtx context.Context, uid int64, platform int32, deviceID string) (int32, error) {
		if c.devices == nil {
			return 1, nil
		}
		slot, slotErr := c.devices.EnsureSlot(slotCtx, uid, int16(platform), deviceID)
		return int32(slot), slotErr
	}
	res, err := service.login(ctx.Context(), req)
	if err != nil {
		c.respond(ctx, contract.OpLogin, nil, err)
		return err
	}
	c.fillSyncState(ctx, res)
	if uid, ok := loginUID(res); ok {
		c.fillActiveCall(ctx.Context(), uid, res)
		c.registerDevice(ctx.Context(), uid, req)
		// BindGate also sets the node event UID. router.Bind has already
		// registered the device key; this idempotent call closes the small
		// window where the first business frame can arrive with an empty UID.
		slot := int32(1)
		if c.devices != nil {
			if resolved, slotErr := c.devices.EnsureSlot(ctx.Context(), uid, int16(req.GetPlatform()), req.GetDeviceId()); slotErr == nil {
				slot = int32(resolved)
			}
		}
		if bindErr := ctx.BindGate(multi.Key(uid, req.GetPlatform(), slot)); bindErr != nil {
			log.Warnf("login gate bind failed: uid = %d, platform = %d, slot = %d, err = %v",
				uid, req.GetPlatform(), slot, bindErr)
		}
	}

	log.Infof("login ok uid=%s platform=%d cid=%d", res.GetUid(), req.GetPlatform(), ctx.CID())
	metrics.Default.Inc("zim_login_total", map[string]string{"result": "ok"})
	payload, err := proto.Marshal(res)
	if err != nil {
		c.respond(ctx, contract.OpLogin, nil, errs.ErrInternal.WithCause(err))
		return err
	}

	c.respond(ctx, contract.OpLogin, payload, nil)
	return nil
}

// verifyToken calls the auth mesh through a typed client. The context timeout
// is always honoured, including transport failures.
func (c *core) verifyToken(ctx context.Context, args *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) {
	client, err := authsvc.NewClient(c.proxy.NewMeshClient)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, authCallTimeout)
	reply, err := client.VerifyToken(callCtx, args)
	cancel()
	return reply, err
}

// bindDevice binds the device key through the zimfw/route locator wrapper. The
// gateway ID comes from the current connection, not from a process constant.
func (c *core) bindDevice(ctx context.Context, nodeCtx loginContext, uid int64, platform int32, slot int32) error {
	return c.binds.Do(ctx, strconv.FormatInt(uid, 10), func() error {
		return c.doBindDevice(ctx, nodeCtx, uid, platform, slot)
	})
}

func (c *core) doBindDevice(ctx context.Context, nodeCtx loginContext, uid int64, platform int32, slot int32) error {
	router, err := route.New(c.proxy, nodeCtx.GID(), route.WithConnRegistry(c.conns))
	if err != nil {
		return err
	}
	if oldGate, locateErr := router.Locate(ctx, uid, platform, slot); locateErr == nil && oldGate != "" {
		payload := []byte{0x08, 0x01, 0x10, byte(platform)}
		if envelope, encodeErr := contract.EncodeEnvelope(&contract.Envelope{
			Opcode: contract.OpPushKick, Payload: payload,
		}); encodeErr == nil {
			_ = c.proxy.Push(ctx, &cluster.PushArgs{
				Kind: cluster.User, Target: multi.Key(uid, platform, slot),
				Message: &cluster.Message{Route: contract.OpPushKick, Data: envelope},
			})
		}
	}
	return router.Bind(ctx, nodeCtx.CID(), uid, platform, slot)
}

func (c *core) respond(ctx loginContext, opcode int32, payload []byte, err error) {
	envelope := &contract.Envelope{Opcode: opcode, Payload: payload}
	if err != nil {
		business := errs.From(err)
		envelope.Code = int32(business.Code().Code())
		envelope.Message = business.Message()
	}
	if responseErr := ctx.Response(envelope); responseErr != nil {
		log.Errorf("login response failed: cid = %d, route = %d, err = %v", ctx.CID(), opcode, responseErr)
	}
}

// WithReactions wires the emoji reaction service (op:1515/1516).
func (c *core) WithReactions(service *reactionsvc.Service) *core {
	c.reactions = service
	return c
}

// WithRTCClientURL sets the SFU URL handed to browsers (ws://host:7880).
func (c *core) WithRTCClientURL(url string) *core {
	if url != "" {
		c.rtcClientURL = url
	}
	return c
}

// WithCache wires the Redis client used by DLP debounce and burn pushes.
func (c *core) WithCache(client redis.UniversalClient) *core {
	c.cache = client
	return c
}

// WithDLPAudit wires the screenshot/record audit publisher.
func (c *core) WithDLPAudit(publisher DLPAuditPublisher) *core {
	c.dlpAudit = publisher
	return c
}

// pushDeviceKeys sends one frame to all device keys in a single multicast.
func (c *core) pushDeviceKeys(ctx context.Context, keys []int64, opcode int32, payload []byte, disconnect bool) {
	if len(keys) == 0 {
		return
	}
	envelope, err := contract.EncodeEnvelope(&contract.Envelope{Opcode: opcode, Payload: payload})
	if err != nil {
		return
	}
	pushCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err = c.proxy.Multicast(pushCtx, &cluster.MulticastArgs{
		Kind: cluster.User, Targets: keys, Disconnect: disconnect,
		Message: &cluster.Message{Route: opcode, Data: envelope},
	}); err != nil {
		// A single stale device key sinks the whole multicast: rebuild it as
		// per-target pushes so the live devices still receive the frame. That
		// stale-key case used to silently drop typing/read/call pushes.
		log.Warnf("device multicast failed: opcode=%d targets=%d err=%v; retrying per target",
			opcode, len(keys), err)
		for _, key := range keys {
			targetCtx, targetCancel := context.WithTimeout(ctx, time.Second)
			_ = c.proxy.Push(targetCtx, &cluster.PushArgs{
				Kind: cluster.User, Target: key,
				Message: &cluster.Message{Route: opcode, Data: envelope},
			})
			targetCancel()
		}
	}
}

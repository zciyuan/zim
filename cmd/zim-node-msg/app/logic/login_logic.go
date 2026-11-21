package logic

import (
	"context"
	"strconv"
	"time"

	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	authpb "github.com/zciyuan/ZIM/internal/service/auth/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

type verifyTokenFunc func(ctx context.Context, args *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error)
type bindDeviceFunc func(ctx context.Context, uid int64, platform int32, slot int32) error
type resolveSlotFunc func(ctx context.Context, uid int64, platform int32, deviceID string) (int32, error)

// loginService implements the op:1001 use case without due/node dependencies.

func newLoginService(verify verifyTokenFunc, bind bindDeviceFunc) *loginService {
	return &loginService{verify: verify, bind: bind, now: time.Now}
}

func newLoginServiceWithClock(verify verifyTokenFunc, bind bindDeviceFunc, now func() time.Time) *loginService {
	service := newLoginService(verify, bind)
	if now != nil {
		service.now = now
	}
	return service
}

func (s *loginService) login(ctx context.Context, req *authwire.LoginReq) (*authwire.LoginRes, error) {
	if req == nil || req.GetToken() == "" {
		return nil, errs.ErrUnauthorized
	}
	if !multi.ValidPlatform(req.GetPlatform()) {
		return nil, errs.ErrUnauthorized
	}

	verified, err := s.verify(ctx, &authpb.VerifyTokenArgs{
		Token:    req.GetToken(),
		Platform: req.GetPlatform(),
		Sdk:      req.GetSdk(),
	})
	if err != nil || verified == nil {
		return nil, errs.ErrUnauthorized
	}
	if verified.GetUid() <= 0 || verified.GetPlatform() != req.GetPlatform() {
		return nil, errs.ErrUnauthorized
	}

	slot := int32(1)
	if s.slot != nil {
		resolved, resolveErr := s.slot(ctx, verified.GetUid(), verified.GetPlatform(), req.GetDeviceId())
		if resolveErr != nil {
			return nil, errs.ErrInternal.WithCause(resolveErr)
		}
		slot = resolved
	}
	if err = s.bind(ctx, verified.GetUid(), verified.GetPlatform(), slot); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}

	return &authwire.LoginRes{
		Uid:        strconv.FormatInt(verified.GetUid(), 10),
		ServerTime: s.now().UnixMilli(),
		Sync:       &authwire.SyncState{},
		OnlineDevices: []*authwire.OnlineDevice{{
			Platform:   req.GetPlatform(),
			DeviceName: req.GetDeviceName(),
		}},
	}, nil
}

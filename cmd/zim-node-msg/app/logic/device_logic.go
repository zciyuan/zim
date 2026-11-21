package logic

import (
	"context"
	"strconv"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/model"
	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
)

// registerDevice records the login device for offline push (ADR-0024).
// Failures only warn: login must not depend on the device registry.
func (c *core) registerDevice(ctx context.Context, uid int64, req *authwire.LoginReq) {
	if c.devices == nil {
		return
	}
	device := &model.Device{
		OwnerID:    uid,
		Platform:   int16(req.GetPlatform()),
		DeviceID:   req.GetDeviceId(),
		DeviceName: req.GetDeviceName(),
		AppVersion: req.GetSdk(),
	}
	if err := c.devices.Register(ctx, device); err != nil {
		log.Warnf("register device failed: uid = %d, platform = %d, err = %v",
			uid, req.GetPlatform(), err)
	}
}

// loginUID parses the uid returned by the login use case.
func loginUID(res *authwire.LoginRes) (int64, bool) {
	if res == nil {
		return 0, false
	}
	uid, err := strconv.ParseInt(res.GetUid(), 10, 64)
	if err != nil {
		return 0, false
	}
	return uid, true
}

// Package route is the thin ZIM adapter over due's locate API. It hides the
// deviceKey encoding from business code and keeps the gateway ID in one place.
package route

import (
	"context"
	"sync"
	"time"

	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/zimfw/multi"
)

// ErrBindNotVisible reports a bind that the locator has not made visible.
var ErrBindNotVisible = errors.New("gate binding not visible")

// ErrLocatorMissing reports a router constructed without a locator.
var ErrLocatorMissing = errors.New("locator missing")

// ErrGatewayMissing reports a router constructed without a gateway ID.
var ErrGatewayMissing = errors.New("gateway id missing")

// bindSession amortizes repeated reconnect binds on the same gateway.
type bindSession struct {
	gid    string
	expire int64
}

var bindSessions sync.Map

// Locator is the subset of due's node proxy used by ZIM.
type Locator interface {
	// LocateGate returns the gateway ID that owns uid.
	LocateGate(ctx context.Context, uid int64) (string, error)
	// BindGate binds cid to gid for uid.
	BindGate(ctx context.Context, gid string, cid, uid int64) error
	// UnbindGate removes the location of uid.
	UnbindGate(ctx context.Context, uid int64) error
}

// New returns a router. Both arguments are required.
func New(locator Locator, gid string, opts ...Option) (*Router, error) {
	if locator == nil {
		return nil, ErrLocatorMissing
	}
	if gid == "" {
		return nil, ErrGatewayMissing
	}
	router := &Router{locator: locator, gid: gid}
	for _, opt := range opts {
		opt(router)
	}
	return router, nil
}

// Bind registers the device's connection on the current gateway and waits for
// the locator to make the binding visible. The wait closes the login/push race:
// a client may send immediately after LoginRes, while the locator write can
// still be in flight.
func (r *Router) Bind(ctx context.Context, cid, uid int64, platform int32, slot int32) error {
	if err := multi.Validate(uid, platform, slot); err != nil {
		return err
	}
	deviceKey := multi.Key(uid, platform, slot)
	if value, ok := bindSessions.Load(deviceKey); ok {
		session := value.(bindSession)
		if session.gid == r.gid && session.expire > time.Now().UnixMilli() {
			// Same gateway reconnect: refresh the cid binding only. Locate and
			// udev entries are already present, so avoid the extra Redis writes.
			if err := r.locator.BindGate(ctx, r.gid, cid, deviceKey); err == nil {
				return nil
			}
		}
	}
	if err := r.locator.BindGate(ctx, r.gid, cid, deviceKey); err != nil {
		return err
	}
	for attempt := 0; attempt < 10; attempt++ {
		gid, err := r.locator.LocateGate(ctx, deviceKey)
		if err == nil && gid == r.gid {
			bindSessions.Store(deviceKey, bindSession{gid: r.gid, expire: time.Now().Add(10 * time.Second).UnixMilli()})
			if r.registry != nil {
				if indexErr := r.registry.Add(ctx, r.gid, deviceKey); indexErr != nil {
					log.Warnf("index connection binding failed: gid = %s, device = %d, err = %v",
						r.gid, deviceKey, indexErr)
				}
				if indexErr := r.registry.AddUserDevice(ctx, uid, deviceKey); indexErr != nil {
					log.Warnf("index user device failed: uid = %d, device = %d, err = %v",
						uid, deviceKey, indexErr)
				}
			}
			return nil
		}
		if attempt == 9 {
			return ErrBindNotVisible
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return ErrBindNotVisible
}

// Locate returns the gateway ID for the device, or an empty string when the
// device is offline.
func (r *Router) Locate(ctx context.Context, uid int64, platform int32, slot int32) (string, error) {
	if err := multi.Validate(uid, platform, slot); err != nil {
		return "", err
	}
	return r.locator.LocateGate(ctx, multi.Key(uid, platform, slot))
}

// Unbind removes the device's location.
func (r *Router) Unbind(ctx context.Context, uid int64, platform int32, slot int32) error {
	if err := multi.Validate(uid, platform, slot); err != nil {
		return err
	}
	deviceKey := multi.Key(uid, platform, slot)
	bindSessions.Delete(deviceKey)
	// The gateway drops the location itself when a connection closes, so a
	// missing entry is expected on logout-after-disconnect. The index cleanup
	// below must still run, otherwise the user stays "online" forever and every
	// later push is sent to a dead device key.
	if err := r.locator.UnbindGate(ctx, deviceKey); err != nil && !errors.Is(err, errors.ErrNotFoundUserLocation) {
		log.Warnf("unbind gate location failed: device = %d, err = %v", deviceKey, err)
	}
	if r.registry != nil {
		if err := r.registry.Remove(ctx, r.gid, deviceKey); err != nil {
			log.Warnf("unindex connection binding failed: gid = %s, device = %d, err = %v",
				r.gid, deviceKey, err)
		}
		if err := r.registry.RemoveUserDevice(ctx, uid, deviceKey); err != nil {
			log.Warnf("unindex user device failed: uid = %d, device = %d, err = %v",
				uid, deviceKey, err)
		}
	}
	return nil
}

// Gateway returns the configured gateway ID.
func (r *Router) Gateway() string {
	return r.gid
}

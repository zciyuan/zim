// Package multi implements the ZIM multi-device identity scheme.
//
// due binds one session per uid. ZIM therefore uses
// deviceKey = uid<<11 | platform<<8 | slot as the due uid, keeping one
// connection per device instance without forking due.
package multi

import (
	"github.com/dobyte/due/v2/errors"
)

const (
	// PlatformShift is the bit offset of the platform field.
	PlatformShift = 8
	// SlotShift is the bit offset of the device slot field.
	SlotShift = 13
	// MaxSlot is the largest device slot.
	MaxSlot = 255
	// MaxUID is the largest uid that fits the 52-bit uid field.
	MaxUID = (1 << 50) - 1
)

// Supported platforms. Values are part of the client protocol and frozen.
const (
	PlatformIOS     int32 = 1
	PlatformAndroid int32 = 2
	PlatformPC      int32 = 3
	PlatformWeb     int32 = 4
	PlatformPad     int32 = 5

	PlatformWindows     int32 = 6
	PlatformMacOS       int32 = 7
	PlatformLinux       int32 = 8
	PlatformIPadOS      int32 = 9
	PlatformAndroidPad  int32 = 10
	PlatformMiniProgram int32 = 11
	PlatformWatch       int32 = 12
	PlatformTV          int32 = 13
	PlatformCar         int32 = 14
	PlatformCLI         int32 = 15
	PlatformVR          int32 = 16
	PlatformBot         int32 = 17
	MaxPlatform         int32 = 31
)

// ErrInvalidUID reports a user ID that cannot be encoded as a device key.
var ErrInvalidUID = errors.New("invalid uid")

// ErrInvalidPlatform reports an unsupported platform value.
var ErrInvalidPlatform = errors.New("invalid platform")

// ErrInvalidSlot reports a device slot outside 1..255.
var ErrInvalidSlot = errors.New("invalid device slot")

// ErrInvalidDeviceKey reports a malformed device key.
var ErrInvalidDeviceKey = errors.New("invalid device key")

// ValidPlatform reports whether platform is a documented client platform.
func ValidPlatform(platform int32) bool {
	return platform >= PlatformIOS && platform <= MaxPlatform
}

// Key returns uid<<11|platform<<8|slot, or zero when any argument is invalid.
func Key(uid int64, platform int32, slot int32) int64 {
	if uid <= 0 || uid > MaxUID || !ValidPlatform(platform) || slot < 1 || slot > MaxSlot {
		return 0
	}
	return uid<<SlotShift | int64(platform)<<PlatformShift | int64(slot)
}

// Split returns the uid and platform encoded in key. Use SplitSlot when the
// device slot is needed.
func Split(key int64) (int64, int32) {
	uid, platform, _ := SplitSlot(key)
	return uid, platform
}

// SplitSlot returns the uid, platform and slot encoded in key.
func SplitSlot(key int64) (int64, int32, int32) {
	return key >> SlotShift, int32(key>>PlatformShift) & 0x1F, int32(key & MaxSlot)
}

// Parse validates key and returns the uid and platform.
func Parse(key int64) (int64, int32, error) {
	uid, platform, _, err := ParseSlot(key)
	return uid, platform, err
}

// ParseSlot validates key and returns the uid, platform and slot.
func ParseSlot(key int64) (int64, int32, int32, error) {
	if key <= 0 {
		return 0, 0, 0, ErrInvalidDeviceKey
	}
	uid, platform, slot := SplitSlot(key)
	if uid <= 0 || uid > MaxUID || !ValidPlatform(platform) || slot < 1 || slot > MaxSlot {
		return 0, 0, 0, ErrInvalidDeviceKey
	}
	if Key(uid, platform, slot) != key {
		return 0, 0, 0, ErrInvalidDeviceKey
	}
	return uid, platform, slot, nil
}

// Validate checks uid, platform and slot.
func Validate(uid int64, platform int32, slot int32) error {
	if !ValidPlatform(platform) {
		return ErrInvalidPlatform
	}
	if slot < 1 || slot > MaxSlot {
		return ErrInvalidSlot
	}
	if uid <= 0 || uid > MaxUID {
		return ErrInvalidUID
	}
	return nil
}

// Platforms returns all supported platform values in ascending order.
func Platforms() []int32 {
	return []int32{
		PlatformIOS, PlatformAndroid, PlatformPC, PlatformWeb, PlatformPad,
		PlatformWindows, PlatformMacOS, PlatformLinux, PlatformIPadOS, PlatformAndroidPad,
		PlatformMiniProgram, PlatformWatch, PlatformTV, PlatformCar, PlatformCLI,
		PlatformVR, PlatformBot,
	}
}

// Name returns the lowercase platform name used in logs and metrics.
func Name(platform int32) string {
	switch platform {
	case PlatformIOS:
		return "ios"
	case PlatformAndroid:
		return "android"
	case PlatformPC:
		return "pc"
	case PlatformWeb:
		return "web"
	case PlatformPad:
		return "pad"
	case PlatformWindows:
		return "windows"
	case PlatformMacOS:
		return "macos"
	case PlatformLinux:
		return "linux"
	case PlatformIPadOS:
		return "ipados"
	case PlatformAndroidPad:
		return "androidpad"
	case PlatformMiniProgram:
		return "miniprogram"
	case PlatformWatch:
		return "watch"
	case PlatformTV:
		return "tv"
	case PlatformCar:
		return "car"
	case PlatformCLI:
		return "cli"
	case PlatformVR:
		return "vr"
	case PlatformBot:
		return "bot"
	default:
		return "unknown"
	}
}

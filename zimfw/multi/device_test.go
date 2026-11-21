package multi

import "testing"

const testUID int64 = 10001

func TestKeySplitSlotRoundTrip(t *testing.T) {
	for _, platform := range Platforms() {
		for _, slot := range []int32{1, 2, 127, MaxSlot} {
			key := Key(testUID, platform, slot)
			uid, gotPlatform, gotSlot, err := ParseSlot(key)
			if err != nil {
				t.Fatalf("ParseSlot(%d): %v", key, err)
			}
			if uid != testUID || gotPlatform != platform || gotSlot != slot {
				t.Fatalf("ParseSlot = (%d,%d,%d), want (%d,%d,%d)",
					uid, gotPlatform, gotSlot, testUID, platform, slot)
			}
			if uid2, platform2 := Split(key); uid2 != testUID || platform2 != platform {
				t.Fatalf("Split = (%d,%d)", uid2, platform2)
			}
		}
	}
}

func TestKeyValues(t *testing.T) {
	cases := []struct {
		uid      int64
		platform int32
		slot     int32
		want     int64
	}{
		{1, PlatformIOS, 1, 1<<13 | 1<<8 | 1},
		{1, PlatformAndroid, 2, 1<<13 | 2<<8 | 2},
		{10001, PlatformWeb, 7, 10001<<13 | 4<<8 | 7},
		{MaxUID, PlatformPad, MaxSlot, MaxUID<<13 | 5<<8 | MaxSlot},
	}
	for _, tc := range cases {
		if got := Key(tc.uid, tc.platform, tc.slot); got != tc.want {
			t.Fatalf("Key(%d,%d,%d) = %d, want %d", tc.uid, tc.platform, tc.slot, got, tc.want)
		}
	}
}

func TestKeyRejectsInvalidInput(t *testing.T) {
	cases := [][3]int64{
		{0, int64(PlatformIOS), 1},
		{-1, int64(PlatformIOS), 1},
		{1, 0, 1},
		{1, 32, 1},
		{1, int64(PlatformIOS), 0},
		{1, int64(PlatformIOS), 256},
		{MaxUID + 1, int64(PlatformIOS), 1},
	}
	for _, tc := range cases {
		if got := Key(tc[0], int32(tc[1]), int32(tc[2])); got != 0 {
			t.Fatalf("Key(%v) = %d, want 0", tc, got)
		}
	}
}

func TestParseRejectsInvalidKeys(t *testing.T) {
	keys := []int64{0, -1, 1, 8, 255}
	for _, key := range keys {
		if _, _, _, err := ParseSlot(key); err == nil {
			t.Fatalf("ParseSlot(%d) succeeded", key)
		}
	}
}

func TestValidate(t *testing.T) {
	if err := Validate(testUID, PlatformIOS, 1); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := Validate(0, PlatformIOS, 1); err != ErrInvalidUID {
		t.Fatalf("uid err = %v", err)
	}
	if err := Validate(testUID, 99, 1); err != ErrInvalidPlatform {
		t.Fatalf("platform err = %v", err)
	}
	if err := Validate(testUID, PlatformIOS, 0); err != ErrInvalidSlot {
		t.Fatalf("slot err = %v", err)
	}
}

func TestPlatformsAndNames(t *testing.T) {
	names := []string{"ios", "android", "pc", "web", "pad", "windows", "macos", "linux", "ipados", "androidpad", "miniprogram", "watch", "tv", "car", "cli", "vr", "bot"}
	for i, platform := range Platforms() {
		if !ValidPlatform(platform) || Name(platform) != names[i] {
			t.Fatalf("platform %d name = %q", platform, Name(platform))
		}
	}
	if Name(99) != "unknown" {
		t.Fatalf("Name(99) = %q", Name(99))
	}
}

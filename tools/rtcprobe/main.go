// Command rtcprobe verifies a real LiveKit endpoint through the ZIM provider.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/zciyuan/ZIM/zimfw/rtc"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:7880", "LiveKit server URL")
	key := flag.String("key", "devkey", "LiveKit API key")
	secret := flag.String("secret", "zimdevsecret012345678901234567890123456789", "LiveKit API secret")
	roomName := flag.String("room", "zim-probe", "room name")
	flag.Parse()

	provider := rtc.NewLiveKit(rtc.LiveKitConfig{URL: *url, APIKey: *key, APISecret: *secret})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	room, err := provider.CreateRoom(ctx, rtc.RoomReq{Room: *roomName, CallID: "probe"})
	if err != nil {
		panic(err)
	}
	token, err := provider.IssueToken(ctx, *roomName, 10001, rtc.RolePublisher)
	if err != nil {
		panic(err)
	}
	if err = provider.RemoveParticipant(ctx, *roomName, 10001); err != nil {
		panic(err)
	}
	fmt.Printf("room=%s sid=%s token_len=%d\n", room.Name, room.ID, len(token))
}

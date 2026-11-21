// Command rtcweb serves a two-participant LiveKit media probe.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/zimfw/rtc"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8099", "HTTP listen address")
	livekitURL := flag.String("livekit", "http://127.0.0.1:7880", "LiveKit server URL")
	key := flag.String("key", "devkey", "LiveKit API key")
	secret := flag.String("secret", "zimdevsecret012345678901234567890123456789", "LiveKit API secret")
	sdk := flag.String("sdk", "run/rtcweb/node_modules/livekit-client/dist/livekit-client.umd.js", "LiveKit UMD bundle")
	roomName := flag.String("room", "zim-media-probe", "room name")
	flag.Parse()

	provider := rtc.NewLiveKit(rtc.LiveKitConfig{URL: *livekitURL, APIKey: *key, APISecret: *secret})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := provider.CreateRoom(ctx, rtc.RoomReq{Room: *roomName, CallID: "media"}); err != nil {
		log.Fatalf("create room: %v", err)
	}
	cancel()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, page)
	})
	http.HandleFunc("/sdk.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, *sdk)
	})
	http.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		uid := int64(10001)
		if r.URL.Query().Get("uid") == "2" {
			uid = 10002
		}
		token, err := provider.IssueToken(r.Context(), *roomName, uid, rtc.RolePublisher)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		body, _ := json.Marshal(map[string]string{"url": strings.Replace(*livekitURL, "http", "ws", 1), "token": token})
		_, _ = w.Write(body)
	})
	log.Printf("rtc media probe on http://%s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

const page = `<!doctype html>
<meta charset="utf-8">
<div id="out">starting</div>
<script src="/sdk.js"></script>
<script>
const out = document.getElementById('out');
const result = (text) => { out.textContent = text; document.title = text; console.log(text); };
async function token(uid) {
  const r = await fetch('/token?uid=' + uid);
  const j = await r.json();
  j.url = 'ws://127.0.0.1:7880';
  return j;
}
async function main() {
  const LK = window.LivekitClient;
  if (!LK) { result('FAIL sdk missing'); return; }
  const t1 = await token(1), t2 = await token(2);
  const room1 = new LK.Room();
  const room2 = new LK.Room();
  const kinds1 = [];
  room1.on(LK.RoomEvent.TrackSubscribed, (track) => { kinds1.push(track.kind); });
  await room1.connect(t1.url, t1.token, { autoSubscribe: true });
  await room1.localParticipant.setMicrophoneEnabled(true);
  await room1.localParticipant.setCameraEnabled(true);
  await room2.connect(t2.url, t2.token, { autoSubscribe: true });
  await room2.localParticipant.setMicrophoneEnabled(true);
  await room2.localParticipant.setCameraEnabled(true);
  for (let i = 0; i < 30 && (room1.remoteParticipants.size === 0 || kinds1.length < 2); i++) {
    await new Promise((r) => setTimeout(r, 500));
  }
  const remote = room1.remoteParticipants.size;
  const media = kinds1.includes('audio') && kinds1.includes('video');
  result((remote > 0 && media ? 'PASS' : 'FAIL') +
    ' room1_remote=' + remote + ' room1_subscribed=' + kinds1.join(',') +
    ' room2_remote=' + room2.remoteParticipants.size);
  await room1.disconnect();
  await room2.disconnect();
}
main().catch((e) => result('FAIL ' + e));
</script>
`

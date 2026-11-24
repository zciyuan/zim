// Command seede2e seeds a conversation between the dev users alice(10001) and
// bob(10002) so the browser e2e suites have rows to open. Dev-only.
package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/network"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/component/token"
	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	msgwire "github.com/zciyuan/ZIM/internal/protocol/message"
	"github.com/zciyuan/ZIM/zimfw/contract"
)

func main() {
	gate := flag.String("gate", "ws://127.0.0.1:3553/ws", "gate URL")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	flag.Parse()
	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		fail("issuer: %v", err)
	}
	contract.InstallPacker()

	alice, frames := connect(*gate, issuer, 10001, "seed-alice")
	defer alice.Close()
	bob, bobFrames := connect(*gate, issuer, 10002, "seed-bob")
	defer bob.Close()

	for i, text := range []string{"你好 Bob，来自 Alice 的第一条消息", "e2e 会话已就绪 ✅"} {
		send(alice, uint32(i+1), contract.OpSendSingle, marshal(&msgwire.SendSingleReq{
			ClientMsgId: fmt.Sprintf("seed-%d-%d", 10001, i),
			RecvId:      10002, MsgType: 1, Content: fmt.Sprintf(`{"text":%q}`, text),
		}))
		if env := envelope(wait(frames, contract.OpSendSingle, 10*time.Second)); env.Code != 0 {
			fail("alice send code=%d msg=%s", env.Code, env.Message)
		}
		wait(bobFrames, contract.OpPushMsg, 10*time.Second)
		fmt.Printf("seeded alice->bob: %s\n", text)
	}
	fmt.Println("PASS conversation seeded")
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func connect(gate string, issuer *token.Issuer, uid int64, deviceID string) (network.Conn, chan *contract.Frame) {
	pair, err := issuer.Issue(uid, 2, deviceID)
	if err != nil {
		fail("issue: %v", err)
	}
	frames := make(chan *contract.Frame, 128)
	q := url.Values{"token": {pair.AccessToken}, "platform": {"2"}, "sdk": {"seede2e"}}
	client := ws.NewClient(ws.WithClientUrl(gate+"?"+q.Encode()), ws.WithClientHeartbeatInterval(0))
	client.OnReceive(func(_ network.Conn, buf buffer.Buffer) {
		defer buf.Release()
		frame, decodeErr := contract.DecodeFrameBytes(buf.Bytes())
		if decodeErr == nil {
			frames <- frame
		}
	})
	conn, err := client.Dial()
	if err != nil {
		fail("dial: %v", err)
	}
	send(conn, 1, contract.OpLogin, marshal(&authwire.LoginReq{
		Token: pair.AccessToken, Platform: 2, Sdk: "seede2e", DeviceId: deviceID,
	}))
	if env := envelope(wait(frames, contract.OpLogin, 5*time.Second)); env.Code != 0 {
		fail("login %s code=%d", deviceID, env.Code)
	}
	return conn, frames
}

func send(conn network.Conn, seq uint32, opcode int32, payload []byte) {
	env, _ := contract.EncodeEnvelope(&contract.Envelope{Opcode: opcode, Payload: payload})
	frame, err := contract.EncodeFrame(contract.Frame{Version: contract.Version1, Opcode: opcode, Seq: seq, Body: env})
	if err != nil {
		fail("frame: %v", err)
	}
	if err = conn.Push(buffer.NewBytes(frame, true)); err != nil {
		fail("push: %v", err)
	}
}

func wait(frames chan *contract.Frame, opcode int32, timeout time.Duration) *contract.Frame {
	deadline := time.After(timeout)
	for {
		select {
		case frame := <-frames:
			if frame.Opcode == opcode {
				return frame
			}
		case <-deadline:
			fail("timeout waiting for opcode=%d", opcode)
		}
	}
}

func envelope(frame *contract.Frame) *contract.Envelope {
	env, err := contract.DecodeEnvelope(frame.Body, frame.Opcode)
	if err != nil {
		fail("decode envelope: %v", err)
	}
	return env
}

func marshal(message proto.Message) []byte {
	payload, err := proto.Marshal(message)
	if err != nil {
		fail("marshal: %v", err)
	}
	return payload
}

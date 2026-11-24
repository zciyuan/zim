// Command rpprobe sends a red-packet card message (1501, msg_type=7, ext_type=1)
// from alice to bob and prints the server response code. Dev-only probe.
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
	conn, frames := connect(*gate, issuer, 10001, "rp-probe")
	defer conn.Close()
	card := fmt.Sprintf(`{"v":1,"rp_id":"%d","sender_uid":"10001","greeting":"probe","rp_type":1,"total_amount":"1000000","total_count":1,"status":1,"expire_at":"0","created_at":"0"}`, time.Now().UnixNano()%1000000)
	send(conn, 2, contract.OpSendSingle, marshal(&msgwire.SendSingleReq{
		ClientMsgId: fmt.Sprintf("rpprobe-%d", time.Now().UnixNano()),
		RecvId:      10002, MsgType: 7, Content: `{"text":"probe card"}`,
		ExtType: 1, ExtPayload: card,
	}))
	env := envelope(wait(frames, contract.OpSendSingle, 10*time.Second))
	fmt.Printf("send code=%d msg=%q\n", env.Code, env.Message)
	if env.Code != 0 {
		os.Exit(1)
	}
	fmt.Println("PASS ext card accepted")
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
	q := url.Values{"token": {pair.AccessToken}, "platform": {"2"}, "sdk": {"rpprobe"}}
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
		Token: pair.AccessToken, Platform: 2, Sdk: "rpprobe", DeviceId: deviceID,
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

// Command devprobe verifies two devices of one account coexist and both receive pushes.
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
	gate2 := flag.String("gate2", "", "optional second gate for the sender")
	hold := flag.Duration("hold", 0, "keep connections open after the check")
	expectKick := flag.Bool("expect-kick", false, "wait for the admin 1902 kick")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	flag.Parse()
	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		fail("issuer: %v", err)
	}
	contract.InstallPacker()
	receiverA, framesA := connect(*gate, issuer, 950000, "probe-a")
	receiverGate := *gate
	if *gate2 != "" {
		receiverGate = *gate2
	}
	receiverB, framesB := connect(receiverGate, issuer, 950000, "probe-b")
	senderGate := *gate
	if *gate2 != "" {
		senderGate = *gate2
	}
	sender, senderFrames := connect(senderGate, issuer, 950001, "probe-sender")

	send(sender, 1, contract.OpSendSingle, marshal(&msgwire.SendSingleReq{
		ClientMsgId: fmt.Sprintf("probe-%d", time.Now().UnixNano()),
		RecvId:      950000, MsgType: 1, Content: `{"text":"probe"}`,
	}))
	if env := envelope(wait(senderFrames, contract.OpSendSingle, 5*time.Second)); env.Code != 0 {
		fail("send code=%d", env.Code)
	}
	wait(framesA, contract.OpPushMsg, 5*time.Second)
	wait(framesB, contract.OpPushMsg, 5*time.Second)
	fmt.Println("PASS two devices coexist and both received the push")
	if *expectKick {
		select {
		case frame := <-framesA:
			if frame.Opcode != contract.OpPushKick {
				fail("expected kick, got opcode=%d", frame.Opcode)
			}
			fmt.Println("PASS admin kick received")
		case <-time.After(10 * time.Second):
			fail("timeout waiting for admin kick")
		}
	}
	if *hold > 0 {
		time.Sleep(*hold)
	}
	_ = receiverA.Close()
	_ = receiverB.Close()
	_ = sender.Close()
}

func connect(gate string, issuer *token.Issuer, uid int64, deviceID string) (network.Conn, chan *contract.Frame) {
	pair, err := issuer.Issue(uid, 2, deviceID)
	if err != nil {
		fail("issue: %v", err)
	}
	frames := make(chan *contract.Frame, 128)
	q := url.Values{"token": {pair.AccessToken}, "platform": {"2"}, "sdk": {"devprobe"}}
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
		Token: pair.AccessToken, Platform: 2, Sdk: "devprobe", DeviceId: deviceID,
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
			fail("timeout %d", opcode)
		}
	}
}

func envelope(frame *contract.Frame) *contract.Envelope {
	env := &contract.Envelope{}
	if err := proto.Unmarshal(frame.Body, env); err != nil {
		fail("envelope: %v", err)
	}
	return env
}

func marshal(m proto.Message) []byte {
	raw, err := proto.Marshal(m)
	if err != nil {
		fail("marshal: %v", err)
	}
	return raw
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

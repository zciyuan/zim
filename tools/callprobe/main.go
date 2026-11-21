package main

import (
	"crypto/tls"
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
	rtcwire "github.com/zciyuan/ZIM/internal/protocol/rtc"
	"github.com/zciyuan/ZIM/zimfw/contract"
)

var insecure bool

func main() {
	gate := flag.String("gate", "ws://127.0.0.1:3553/ws", "gate URL")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	callerUID := flag.Int64("caller", 950000, "caller uid")
	calleeUID := flag.Int64("callee", 950001, "callee uid")
	flag.BoolVar(&insecure, "insecure", false, "skip TLS verification")
	flag.Parse()
	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		die("issuer: %v", err)
	}
	contract.InstallPacker()
	caller, cf := connect(*gate, issuer, *callerUID, "probe-caller")
	callee, ff := connect(*gate, issuer, *calleeUID, "probe-callee")
	callID := fmt.Sprintf("probe-%d", time.Now().UnixNano())

	send(caller, 1, contract.OpCallInvite, marshal(&rtcwire.CallInviteReq{
		CallId: callID, Target: &rtcwire.CallTarget{Type: 1, Id: *calleeUID}, Media: 1,
	}))
	if env := env(wait(cf, contract.OpCallInvite, 5*time.Second)); env.Code != 0 {
		die("invite code=%d", env.Code)
	}
	if env := env(wait(ff, contract.OpCallAnswer, 5*time.Second)); env.Code != 0 {
		die("ring code=%d", env.Code)
	}
	fmt.Println("PASS invite/ringing")

	send(callee, 2, contract.OpCallAnswer, marshal(&rtcwire.CallAnswerReq{CallId: callID, Accept: true}))
	if env := env(wait(ff, contract.OpCallAnswer, 5*time.Second)); env.Code != 0 {
		die("answer code=%d", env.Code)
	}
	answered := env(wait(cf, contract.OpCallAnswer, 5*time.Second))
	res := &rtcwire.CallAnswerRes{}
	if err := proto.Unmarshal(answered.Payload, res); err != nil || res.GetCallId() != callID {
		die("answer push: %v", err)
	}
	fmt.Println("PASS answer/notification")

	send(callee, 3, contract.OpCallHangup, marshal(&rtcwire.CallHangupReq{CallId: callID, Reason: 1, Duration: 1}))
	if env := env(wait(ff, contract.OpCallHangup, 5*time.Second)); env.Code != 0 {
		die("hangup code=%d", env.Code)
	}
	if env := env(wait(cf, contract.OpCallHangup, 5*time.Second)); env.Code != 0 {
		die("hangup push code=%d", env.Code)
	}
	fmt.Println("PASS hangup/notification")
	_ = caller.Close()
	_ = callee.Close()
}

func connect(gate string, issuer *token.Issuer, uid int64, device string) (network.Conn, chan *contract.Frame) {
	pair, err := issuer.Issue(uid, 2, device)
	if err != nil {
		die("issue: %v", err)
	}
	frames := make(chan *contract.Frame, 128)
	q := url.Values{"token": {pair.AccessToken}, "platform": {"2"}, "sdk": {device}}
	client := ws.NewClient(ws.WithClientUrl(gate+"?"+q.Encode()), ws.WithClientHeartbeatInterval(0),
		ws.WithClientTLSConfig(&tls.Config{InsecureSkipVerify: insecure}))
	client.OnReceive(func(_ network.Conn, buf buffer.Buffer) {
		defer buf.Release()
		if frame, err := contract.DecodeFrameBytes(buf.Bytes()); err == nil {
			frames <- frame
		}
	})
	conn, err := client.Dial()
	if err != nil {
		die("dial: %v", err)
	}
	send(conn, 1, contract.OpLogin, marshal(&authwire.LoginReq{
		Token: pair.AccessToken, Platform: 2, Sdk: device, DeviceId: device,
	}))
	if e := env(wait(frames, contract.OpLogin, 5*time.Second)); e.Code != 0 {
		die("login code=%d", e.Code)
	}
	return conn, frames
}

func send(conn network.Conn, seq uint32, op int32, payload []byte) {
	e, _ := contract.EncodeEnvelope(&contract.Envelope{Opcode: op, Payload: payload})
	frame, err := contract.EncodeFrame(contract.Frame{Version: contract.Version1, Opcode: op, Seq: seq, Body: e})
	if err != nil {
		die("frame: %v", err)
	}
	if err = conn.Push(buffer.NewBytes(frame, true)); err != nil {
		die("push: %v", err)
	}
}

func wait(frames chan *contract.Frame, op int32, timeout time.Duration) *contract.Frame {
	deadline := time.After(timeout)
	for {
		select {
		case frame := <-frames:
			if frame.Opcode == op {
				return frame
			}
		case <-deadline:
			die("timeout %d", op)
		}
	}
}

func env(frame *contract.Frame) *contract.Envelope {
	e := &contract.Envelope{}
	if err := proto.Unmarshal(frame.Body, e); err != nil {
		die("envelope: %v", err)
	}
	return e
}

func marshal(m proto.Message) []byte {
	raw, err := proto.Marshal(m)
	if err != nil {
		die("marshal: %v", err)
	}
	return raw
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

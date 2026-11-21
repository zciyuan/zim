// Command msgload measures single-chat send-to-push latency end to end.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
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

var insecure bool

func main() {
	gate := flag.String("gate", "ws://127.0.0.1:3553/ws", "gate WebSocket URL")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	text := flag.String("text", "latency", "message text")
	count := flag.Int("messages", 100, "messages to send")
	fromUID := flag.Int64("from", 950000, "sender uid")
	toUID := flag.Int64("to", 950001, "receiver uid")
	verifySeq := flag.Bool("verify-seq", false, "require seq +1")
	flag.BoolVar(&insecure, "insecure", false, "skip TLS certificate verification")
	flag.Parse()
	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		fail("issuer: %v", err)
	}
	contract.InstallPacker()
	sender, senderFrames := connect(*gate, issuer, *fromUID)
	receiver, receiverFrames := connect(*gate, issuer, *toUID)

	latencies := make([]time.Duration, 0, *count)
	var lastSeq int64
	for i := 0; i < *count; i++ {
		start := time.Now()
		send(sender, uint32(i+1), contract.OpSendSingle, marshal(&msgwire.SendSingleReq{
			ClientMsgId: fmt.Sprintf("msgload-%d", time.Now().UnixNano()),
			RecvId:      *toUID, MsgType: 1, Content: fmt.Sprintf(`{"text":%q}`, *text),
		}))
		response := wait(senderFrames, contract.OpSendSingle, 5*time.Second)
		env := &contract.Envelope{}
		if err := proto.Unmarshal(response.Body, env); err != nil || env.Code != 0 {
			fail("send code=%d err=%v", env.Code, err)
		}
		res := &msgwire.SendSingleRes{}
		if err := proto.Unmarshal(env.Payload, res); err != nil {
			fail("send payload: %v", err)
		}
		if *verifySeq {
			if lastSeq > 0 && res.GetSeq() != lastSeq+1 {
				fail("seq not monotonic: last=%d current=%d", lastSeq, res.GetSeq())
			}
			lastSeq = res.GetSeq()
		}
		_ = wait(receiverFrames, contract.OpPushMsg, 5*time.Second)
		latencies = append(latencies, time.Since(start))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("messages=%d success=%d p50=%s p95=%s p99=%s max=%s\n", *count, len(latencies),
		pct(latencies, 0.50), pct(latencies, 0.95), pct(latencies, 0.99), latencies[len(latencies)-1].Round(time.Millisecond))
	if pct(latencies, 0.99) > 200*time.Millisecond {
		_ = sender.Close()
		_ = receiver.Close()
		os.Exit(1)
	}
	_ = sender.Close()
	_ = receiver.Close()
}

func connect(gate string, issuer *token.Issuer, uid int64) (network.Conn, chan *contract.Frame) {
	pair, err := issuer.Issue(uid, 2, "msgload")
	if err != nil {
		fail("issue: %v", err)
	}
	frames := make(chan *contract.Frame, 128)
	q := url.Values{"token": {pair.AccessToken}, "platform": {"2"}, "sdk": {"msgload"}}
	client := ws.NewClient(ws.WithClientUrl(gate+"?"+q.Encode()), ws.WithClientHeartbeatInterval(0),
		ws.WithClientTLSConfig(&tls.Config{InsecureSkipVerify: insecure}))
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
		Token: pair.AccessToken, Platform: 2, Sdk: "msgload", DeviceId: "msgload",
	}))
	env := envelope(wait(frames, contract.OpLogin, 5*time.Second))
	if env.Code != 0 {
		fail("login code=%d", env.Code)
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

func pct(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)) * p)
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index].Round(time.Millisecond)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

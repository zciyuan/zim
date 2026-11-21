// Command groupload creates a large group and measures group send latency.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/network"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/component/token"
	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	groupwire "github.com/zciyuan/ZIM/internal/protocol/group"
	msgwire "github.com/zciyuan/ZIM/internal/protocol/message"
	"github.com/zciyuan/ZIM/zimfw/contract"
)

var insecure bool

func main() {
	gate := flag.String("gate", "ws://127.0.0.1:3553/ws", "gate WebSocket URL")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	size := flag.Int("size", 2000, "group member limit")
	messages := flag.Int("messages", 20, "group messages to send")
	flag.BoolVar(&insecure, "insecure", false, "skip TLS verification")
	flag.Parse()
	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		fail("issuer: %v", err)
	}
	contract.InstallPacker()
	conn, frames := connect(*gate, issuer, 960000)
	defer func() { _ = conn.Close() }()

	members := make([]int64, 0, *size-1)
	for i := 1; i < *size; i++ {
		members = append(members, 960000+int64(i))
	}
	started := time.Now()
	send(conn, 1, contract.OpCreateGroup, marshal(&groupwire.CreateGroupReq{
		Name: "load-group", GroupType: 1, MemberIds: members,
	}))
	created := &groupwire.CreateGroupRes{}
	if env := envelope(wait(frames, contract.OpCreateGroup, 10*time.Second)); env.Code != 0 {
		fail("create group code=%d", env.Code)
	} else if err = proto.Unmarshal(env.Payload, created); err != nil {
		fail("create payload: %v", err)
	}
	gid, err := strconv.ParseInt(created.GetGroupId(), 10, 64)
	if err != nil {
		fail("group id: %v", err)
	}
	createLatency := time.Since(started)
	latencies := make([]time.Duration, 0, *messages)
	var lastSeq int64
	for i := 0; i < *messages; i++ {
		start := time.Now()
		send(conn, uint32(i+2), contract.OpSendGroup, marshal(&msgwire.SendGroupReq{
			ClientMsgId: fmt.Sprintf("groupload-%d-%d", gid, i),
			GroupId:     gid, MsgType: 1, Content: `{"text":"group-load"}`,
		}))
		env := envelope(wait(frames, contract.OpSendGroup, 15*time.Second))
		if env.Code != 0 {
			fail("send group code=%d", env.Code)
		}
		res := &msgwire.SendGroupRes{}
		if err = proto.Unmarshal(env.Payload, res); err != nil {
			fail("send payload: %v", err)
		}
		if lastSeq > 0 && res.GetSeq() != lastSeq+1 {
			fail("seq not monotonic: last=%d current=%d", lastSeq, res.GetSeq())
		}
		lastSeq = res.GetSeq()
		latencies = append(latencies, time.Since(start))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("group=%d size=%d messages=%d create=%s p50=%s p95=%s p99=%s max=%s seq_last=%d\n",
		gid, *size, *messages, createLatency.Round(time.Millisecond),
		pct(latencies, 0.50), pct(latencies, 0.95), pct(latencies, 0.99),
		latencies[len(latencies)-1].Round(time.Millisecond), lastSeq)
}

func connect(gate string, issuer *token.Issuer, uid int64) (network.Conn, chan *contract.Frame) {
	pair, err := issuer.Issue(uid, 2, "groupload")
	if err != nil {
		fail("issue: %v", err)
	}
	frames := make(chan *contract.Frame, 64)
	q := url.Values{"token": {pair.AccessToken}, "platform": {"2"}, "sdk": {"groupload"}}
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
		Token: pair.AccessToken, Platform: 2, Sdk: "groupload", DeviceId: "groupload",
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

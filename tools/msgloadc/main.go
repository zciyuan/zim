// Command msgloadc drives concurrent single-chat sends and reports aggregate
// send-ack latency, throughput and push delivery count.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
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
	gate := flag.String("gate", "ws://127.0.0.1:3553/ws", "gate WebSocket URL")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	senders := flag.Int("senders", 20, "concurrent sender connections")
	messages := flag.Int("messages", 50, "messages per sender")
	toUID := flag.Int64("to", 950001, "receiver uid")
	uidStart := flag.Int64("uid-start", 950100, "first synthetic sender uid")
	distinct := flag.Bool("distinct", false, "give each sender its own receiver (to, to+1, ...)")
	verifySeq := flag.Bool("verify-seq", false, "require each sender's response seq to increase by one")
	interval := flag.Duration("interval", 0, "pause between messages per sender (realistic human pace)")
	dialConcurrency := flag.Int("dial-concurrency", 100, "max concurrent sender dials")
	flag.Parse()
	if *senders <= 0 || *messages <= 0 {
		fail("senders and messages must be positive")
	}

	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		fail("issuer: %v", err)
	}
	contract.InstallPacker()

	var (
		receiver  network.Conn
		receivers []network.Conn
		recvMu    sync.Mutex
		pushed    atomic.Int64
		delivery  []time.Duration
		deliverMu sync.Mutex
	)
	recordDelivery := func(frame *contract.Frame) {
		if frame.Opcode != contract.OpPushMsg {
			return
		}
		pushed.Add(1)
		envelope, err := contract.DecodeEnvelope(frame.Body, frame.Opcode)
		if err != nil {
			return
		}
		item := &msgwire.MessageItem{}
		if proto.Unmarshal(envelope.Payload, item) != nil || item.GetSendTime() <= 0 {
			return
		}
		deliverMu.Lock()
		delivery = append(delivery, time.Since(time.UnixMilli(item.GetSendTime())))
		deliverMu.Unlock()
	}
	if !*distinct {
		var receiverFrames chan *contract.Frame
		receiver, receiverFrames = connect(*gate, issuer, *toUID, "msgloadc-recv")
		go drain(receiverFrames, recordDelivery)
	}

	var (
		latencies []time.Duration
		latMu     sync.Mutex
		failures  atomic.Int64
		firstErr  atomic.Pointer[string]
	)
	started := time.Now()
	dialSem := make(chan struct{}, max(1, *dialConcurrency))
	var wg sync.WaitGroup
	for i := 0; i < *senders; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			uid := *uidStart + int64(index)
			target := *toUID
			dialSem <- struct{}{}
			if *distinct {
				target = *toUID + int64(index)
				recv, recvFrames, recvErr := newConn(*gate, issuer, target, fmt.Sprintf("msgloadc-recv-%d", index))
				if recvErr != nil {
					<-dialSem
					failures.Add(1)
					storeErr(&firstErr, "connect receiver: "+recvErr.Error())
					return
				}
				recvMu.Lock()
				receivers = append(receivers, recv)
				recvMu.Unlock()
				go drain(recvFrames, recordDelivery)
			}
			conn, frames, connectErr := newConn(*gate, issuer, uid, fmt.Sprintf("msgloadc-%d", index))
			<-dialSem
			if connectErr != nil {
				failures.Add(1)
				storeErr(&firstErr, "connect: "+connectErr.Error())
				return
			}
			defer func() { _ = conn.Close() }()
			if *interval > 0 {
				// Stagger the first message across one interval so a 1,000
				// sender online population produces ~1,000 msg/s steadily,
				// instead of one artificial burst at t=0.
				offset := time.Duration(index) * *interval / time.Duration(*senders)
				time.Sleep(offset)
			}
			var lastSeq int64
			for seq := 0; seq < *messages; seq++ {
				start := time.Now()
				if err := send(conn, uint32(seq+2), &msgwire.SendSingleReq{
					ClientMsgId: fmt.Sprintf("msgloadc-%d-%d-%d", index, seq, time.Now().UnixNano()),
					RecvId:      target, MsgType: 1, Content: `{"text":"msgloadc"}`,
				}); err != nil {
					failures.Add(1)
					storeErr(&firstErr, err.Error())
					continue
				}
				response, ok := waitOpcode(frames, contract.OpSendSingle, 5*time.Second)
				if !ok {
					failures.Add(1)
					storeErr(&firstErr, "send timeout")
					continue
				}
				if *verifySeq {
					env := &contract.Envelope{}
					res := &msgwire.SendSingleRes{}
					if unmarshalErr := proto.Unmarshal(response.Body, env); unmarshalErr != nil {
						failures.Add(1)
						storeErr(&firstErr, "send response envelope: "+unmarshalErr.Error())
						continue
					}
					if env.Code != 0 {
						failures.Add(1)
						storeErr(&firstErr, fmt.Sprintf("send response code=%d message=%s", env.Code, env.Message))
						continue
					}
					if unmarshalErr := proto.Unmarshal(env.Payload, res); unmarshalErr != nil {
						failures.Add(1)
						storeErr(&firstErr, "send response payload: "+unmarshalErr.Error())
						continue
					}
					if lastSeq > 0 && res.GetSeq() != lastSeq+1 {
						failures.Add(1)
						storeErr(&firstErr, fmt.Sprintf("seq gap: %d -> %d", lastSeq, res.GetSeq()))
						continue
					}
					lastSeq = res.GetSeq()
				}
				latMu.Lock()
				latencies = append(latencies, time.Since(start))
				latMu.Unlock()
				if *interval > 0 && seq < *messages-1 {
					time.Sleep(*interval)
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(started)
	total := *senders * *messages

	deadline := time.Now().Add(10 * time.Second)
	for pushed.Load() < int64(total) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	deliverMu.Lock()
	deliveries := append([]time.Duration(nil), delivery...)
	deliverMu.Unlock()
	sort.Slice(deliveries, func(i, j int) bool { return deliveries[i] < deliveries[j] })
	fmt.Printf("senders=%d messages=%d total=%d success=%d failures=%d pushes=%d elapsed=%s rate=%.0f/s\n",
		*senders, *messages, total, len(latencies), failures.Load(), pushed.Load(),
		elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
	if firstErr.Load() != nil {
		fmt.Printf("first failure: %s\n", *firstErr.Load())
	}
	if len(latencies) > 0 {
		fmt.Printf("send-ack p50=%s p95=%s p99=%s max=%s\n",
			pct(latencies, 0.50), pct(latencies, 0.95), pct(latencies, 0.99),
			latencies[len(latencies)-1].Round(time.Millisecond))
	}
	if len(deliveries) > 0 {
		fmt.Printf("delivery p50=%s p95=%s p99=%s max=%s samples=%d\n",
			pct(deliveries, 0.50), pct(deliveries, 0.95), pct(deliveries, 0.99),
			deliveries[len(deliveries)-1].Round(time.Millisecond), len(deliveries))
	}
	recvMu.Lock()
	for _, conn := range receivers {
		_ = conn.Close()
	}
	recvMu.Unlock()
	if receiver != nil {
		_ = receiver.Close()
	}
	if failures.Load() > 0 || pushed.Load() < int64(total) {
		os.Exit(1)
	}
}

func connect(gate string, issuer *token.Issuer, uid int64, device string) (network.Conn, chan *contract.Frame) {
	conn, frames, err := newConn(gate, issuer, uid, device)
	if err != nil {
		fail("connect uid=%d: %v", uid, err)
	}
	return conn, frames
}

func newConn(gate string, issuer *token.Issuer, uid int64, device string) (network.Conn, chan *contract.Frame, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		conn, frames, err := dialAndLogin(gate, issuer, uid, device)
		if err == nil {
			return conn, frames, nil
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return nil, nil, fmt.Errorf("after 3 attempts: %w", lastErr)
}

func dialAndLogin(gate string, issuer *token.Issuer, uid int64, device string) (network.Conn, chan *contract.Frame, error) {
	pair, err := issuer.Issue(uid, 2, device)
	if err != nil {
		return nil, nil, err
	}
	frames := make(chan *contract.Frame, 4096)
	q := url.Values{"token": {pair.AccessToken}, "platform": {"2"}, "sdk": {"msgloadc"}}
	client := ws.NewClient(ws.WithClientUrl(gate+"?"+q.Encode()), ws.WithClientHeartbeatInterval(0),
		ws.WithClientTLSConfig(&tls.Config{InsecureSkipVerify: true}))
	client.OnReceive(func(_ network.Conn, buf buffer.Buffer) {
		defer buf.Release()
		frame, decodeErr := contract.DecodeFrameBytes(buf.Bytes())
		if decodeErr == nil {
			select {
			case frames <- frame:
			default:
			}
		}
	})
	conn, err := client.Dial()
	if err != nil {
		return nil, nil, err
	}
	if err = send(conn, 1, &authwire.LoginReq{
		Token: pair.AccessToken, Platform: 2, Sdk: "msgloadc", DeviceId: device,
	}); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	loginFrame, ok := waitOpcode(frames, contract.OpLogin, 5*time.Second)
	if !ok {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("login timeout")
	}
	loginEnvelope := &contract.Envelope{}
	if err = proto.Unmarshal(loginFrame.Body, loginEnvelope); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("login envelope: %w", err)
	}
	if loginEnvelope.Code != 0 {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("login code=%d message=%s", loginEnvelope.Code, loginEnvelope.Message)
	}
	// Give the gateway/node binding a beat before the first business frame;
	// a real client has user interaction between login and send.
	time.Sleep(50 * time.Millisecond)
	return conn, frames, nil
}

func drain(frames chan *contract.Frame, fn func(*contract.Frame)) {
	for frame := range frames {
		fn(frame)
	}
}

func send(conn network.Conn, seq uint32, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	env, err := contract.EncodeEnvelope(&contract.Envelope{Opcode: contract.OpSendSingle, Payload: payload})
	if err != nil {
		return err
	}
	opcode := contract.OpSendSingle
	if _, ok := msg.(*authwire.LoginReq); ok {
		opcode = contract.OpLogin
		env, err = contract.EncodeEnvelope(&contract.Envelope{Opcode: opcode, Payload: payload})
		if err != nil {
			return err
		}
	}
	frame, err := contract.EncodeFrame(contract.Frame{Version: contract.Version1, Opcode: opcode, Seq: seq, Body: env})
	if err != nil {
		return err
	}
	return conn.Push(buffer.NewBytes(frame, true))
}

func waitOpcode(frames chan *contract.Frame, opcode int32, timeout time.Duration) (*contract.Frame, bool) {
	deadline := time.After(timeout)
	for {
		select {
		case frame := <-frames:
			if frame.Opcode == opcode {
				return frame, true
			}
		case <-deadline:
			return nil, false
		}
	}
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

func storeErr(slot *atomic.Pointer[string], value string) {
	if slot.Load() == nil {
		copy := value
		slot.CompareAndSwap(nil, &copy)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

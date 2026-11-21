// Command load opens synthetic WebSocket connections against zim-gate and
// reports connect latency. It is a tool, not one of the 11 ZIM binaries.
package main

import (
	"flag"
	"fmt"
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
	"github.com/zciyuan/ZIM/zimfw/contract"
)

func main() {
	gate := flag.String("gate", "ws://127.0.0.1:3553/ws", "gate WebSocket URL")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	count := flag.Int("connections", 1000, "number of connections")
	dialConcurrency := flag.Int("dial-concurrency", 100, "max concurrent dials")
	hold := flag.Duration("hold", 30*time.Second, "how long to keep connections open")
	uidStart := flag.Int64("uid-start", 900000, "first synthetic uid")
	login := flag.Bool("login", false, "send op:1001 and keep the session authorized")
	flag.Parse()

	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	contract.InstallPacker()
	var success, failure atomic.Int64
	latencies := make([]time.Duration, 0, *count)
	var mu sync.Mutex
	var errMu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	sem := make(chan struct{}, *dialConcurrency)
	start := time.Now()
	for i := 0; i < *count; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			pair, issueErr := issuer.Issue(*uidStart+int64(index), 2, "load")
			if issueErr != nil {
				failure.Add(1)
				return
			}
			heartbeat := time.Duration(0)
			if *login {
				heartbeat = 10 * time.Second
			}
			client := ws.NewClient(
				ws.WithClientUrl(fmt.Sprintf("%s?token=%s&platform=2&sdk=load", *gate, pair.AccessToken)),
				ws.WithClientHeartbeatInterval(heartbeat),
			)
			loginCode := make(chan int32, 1)
			if *login {
				client.OnReceive(func(_ network.Conn, buf buffer.Buffer) {
					defer buf.Release()
					frame, decodeErr := contract.DecodeFrameBytes(buf.Bytes())
					if decodeErr != nil || frame.Opcode != contract.OpLogin {
						return
					}
					env := &contract.Envelope{}
					if proto.Unmarshal(frame.Body, env) == nil {
						select {
						case loginCode <- env.Code:
						default:
						}
					}
				})
			}
			sem <- struct{}{}
			connectStart := time.Now()
			conn, dialErr := client.Dial()
			elapsed := time.Since(connectStart)
			<-sem
			if dialErr != nil {
				failure.Add(1)
				errMu.Lock()
				if firstErr == nil {
					firstErr = dialErr
				}
				errMu.Unlock()
				return
			}
			if *login {
				payload, _ := proto.Marshal(&authwire.LoginReq{
					Token: pair.AccessToken, Platform: 2, Sdk: "load",
					DeviceId: fmt.Sprintf("load-%d", index),
				})
				env, _ := contract.EncodeEnvelope(&contract.Envelope{Opcode: contract.OpLogin, Payload: payload})
				frame, _ := contract.EncodeFrame(contract.Frame{
					Version: contract.Version1, Opcode: contract.OpLogin, Seq: 1, Body: env,
				})
				if pushErr := conn.Push(buffer.NewBytes(frame, true)); pushErr != nil {
					failure.Add(1)
					_ = conn.Close()
					return
				}
				select {
				case code := <-loginCode:
					if code != 0 {
						failure.Add(1)
						_ = conn.Close()
						return
					}
				case <-time.After(5 * time.Second):
					failure.Add(1)
					_ = conn.Close()
					return
				}
			}
			success.Add(1)
			mu.Lock()
			latencies = append(latencies, elapsed)
			mu.Unlock()
			time.Sleep(*hold)
			_ = conn.Close()
		}(i)
	}
	wg.Wait()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if firstErr != nil {
		fmt.Printf("first failure: %v\n", firstErr)
	}
	fmt.Printf("connections=%d success=%d failure=%d elapsed=%s\n",
		*count, success.Load(), failure.Load(), time.Since(start).Round(time.Millisecond))
	if len(latencies) > 0 {
		fmt.Printf("connect p50=%s p95=%s p99=%s max=%s\n",
			percentile(latencies, 0.50), percentile(latencies, 0.95),
			percentile(latencies, 0.99), latencies[len(latencies)-1].Round(time.Millisecond))
	}
	if failure.Load() > 0 {
		os.Exit(1)
	}
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)) * p)
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index].Round(time.Millisecond)
}

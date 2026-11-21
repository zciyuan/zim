// Command scaleload opens many local WebSocket connections through loopback
// aliases so a single host can exceed the ephemeral-port limit.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/zciyuan/ZIM/internal/component/token"
	"github.com/zciyuan/ZIM/zimfw/contract"
)

func main() {
	target := flag.String("url", "wss://127.0.0.1:12001/ws", "gate WSS URL")
	tokenValue := flag.String("token", "", "access token")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret when -token is empty")
	uid := flag.Int64("uid", 900001, "synthetic uid when -token is empty")
	n := flag.Int("n", 10000, "connection count")
	concurrency := flag.Int("concurrency", 200, "dial concurrency")
	ipCount := flag.Int("ip-count", 50, "loopback source IP count")
	hold := flag.Duration("hold", 5*time.Second, "hold after dialing")
	flag.Parse()
	accessToken := *tokenValue
	if accessToken == "" {
		issuer, tokenErr := token.New(*secret, time.Hour, time.Hour)
		if tokenErr != nil {
			fail("issuer: %v", tokenErr)
		}
		pair, issueErr := issuer.Issue(*uid, 4, "scaleload")
		if issueErr != nil {
			fail("issue token: %v", issueErr)
		}
		accessToken = pair.AccessToken
	}
	base, err := url.Parse(*target)
	if err != nil {
		fail("url: %v", err)
	}
	q := base.Query()
	q.Set("token", accessToken)
	q.Set("platform", "4")
	q.Set("sdk", "scaleload")
	base.RawQuery = q.Encode()
	url := base.String()

	var opened, failed atomic.Int64
	sem := make(chan struct{}, *concurrency)
	conns := make([]*websocket.Conn, 0, *n)
	var mu sync.Mutex
	var errMu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	started := time.Now()
	for i := 0; i < *n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }()
			dialer := &websocket.Dialer{
				TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
				HandshakeTimeout: 15 * time.Second,
			}
			if *ipCount > 0 {
				ip := net.ParseIP(fmt.Sprintf("127.0.0.%d", index%*ipCount+1))
				dialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
					return (&net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}, Timeout: 15 * time.Second}).DialContext(ctx, network, addr)
				}
			}
			conn, _, dialErr := dialer.Dial(url, nil)
			if dialErr != nil {
				failed.Add(1)
				errMu.Lock()
				if firstErr == nil {
					firstErr = dialErr
				}
				errMu.Unlock()
				return
			}
			opened.Add(1)
			// Drain server heartbeats/pushes; without a reader the kernel
			// send buffer fills and the gate closes idle connections.
			go func(c *websocket.Conn) {
				for {
					messageType, data, readErr := c.ReadMessage()
					if readErr != nil {
						return
					}
					if messageType != websocket.BinaryMessage {
						continue
					}
					frame, decodeErr := contract.DecodeFrameBytes(data)
					if decodeErr != nil || frame.Opcode != 0 {
						continue
					}
					// Reply to the gate's binary heartbeat; idle production
					// clients do the same every 15s.
					raw, encodeErr := contract.EncodeFrame(contract.Frame{
						Version: contract.Version1, Opcode: 0,
					})
					if encodeErr == nil {
						_ = c.WriteMessage(websocket.BinaryMessage, raw)
					}
				}
			}(conn)
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			if total := opened.Load() + failed.Load(); total%1000 == 0 {
				fmt.Printf("progress=%d opened=%d failed=%d\n", total, opened.Load(), failed.Load())
			}
		}(i)
	}
	wg.Wait()
	if firstErr != nil {
		fmt.Printf("first failure: %v\n", firstErr)
	}
	fmt.Printf("target=%d opened=%d failed=%d dial_elapsed=%s\n", *n, opened.Load(), failed.Load(), time.Since(started).Round(time.Millisecond))
	time.Sleep(*hold)
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

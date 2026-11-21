// Command wtload opens concurrent WebTransport sessions against a gate and
// keeps one ZIM bidirectional stream open on each session.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	wt "github.com/quic-go/webtransport-go"

	"github.com/zciyuan/ZIM/internal/component/token"
	"github.com/zciyuan/ZIM/zimfw/contract"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:12002", "gate HTTP/3 address")
	path := flag.String("path", "/wt", "WebTransport path")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	count := flag.Int("sessions", 100, "sessions to open")
	concurrency := flag.Int("concurrency", 20, "parallel dials")
	hold := flag.Duration("hold", 30*time.Second, "how long to keep sessions open")
	uidStart := flag.Int64("uid-start", 990000, "first synthetic uid")
	flag.Parse()
	if *count <= 0 || *concurrency <= 0 {
		fail("sessions and concurrency must be positive")
	}

	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		fail("issuer: %v", err)
	}
	transport := &wt.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // local self-signed cert
		QUICConfig: &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		},
	}
	var (
		success, failure atomic.Int64
		firstErr         atomic.Pointer[string]
		latencies        []time.Duration
		latMu            sync.Mutex
		sessions         []*wt.Session
		sessMu           sync.Mutex
	)
	started := time.Now()
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	for i := 0; i < *count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }()
			uid := *uidStart + int64(index)
			pair, issueErr := issuer.Issue(uid, 4, fmt.Sprintf("wtload-%d", index))
			if issueErr != nil {
				failure.Add(1)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			start := time.Now()
			_, session, dialErr := transport.Dial(ctx,
				fmt.Sprintf("https://%s%s?token=%s&platform=4&sdk=wtload", *addr, *path, pair.AccessToken), nil)
			if dialErr != nil {
				cancel()
				failure.Add(1)
				if firstErr.Load() == nil {
					value := dialErr.Error()
					firstErr.CompareAndSwap(nil, &value)
				}
				return
			}
			stream, streamErr := session.OpenStreamSync(ctx)
			if streamErr != nil {
				cancel()
				_ = session.CloseWithError(0, "stream failed")
				failure.Add(1)
				return
			}
			frame, frameErr := contract.EncodeFrame(contract.Frame{})
			if frameErr == nil {
				_, _ = stream.Write(frame)
			}
			cancel()
			success.Add(1)
			latMu.Lock()
			latencies = append(latencies, time.Since(start))
			latMu.Unlock()
			sessMu.Lock()
			sessions = append(sessions, session)
			sessMu.Unlock()
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(started)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("sessions=%d success=%d failure=%d dial_elapsed=%s\n",
		*count, success.Load(), failure.Load(), elapsed.Round(time.Millisecond))
	if len(latencies) > 0 {
		fmt.Printf("connect p50=%s p95=%s p99=%s max=%s\n",
			pct(latencies, 0.50), pct(latencies, 0.95), pct(latencies, 0.99),
			latencies[len(latencies)-1].Round(time.Millisecond))
	}
	if firstErr.Load() != nil {
		fmt.Printf("first failure: %s\n", *firstErr.Load())
	}
	time.Sleep(*hold)
	sessMu.Lock()
	for _, session := range sessions {
		_ = session.CloseWithError(0, "load done")
	}
	sessMu.Unlock()
	_ = transport.Close()
	if failure.Load() > 0 {
		os.Exit(1)
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

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

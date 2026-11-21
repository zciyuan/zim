// Command friendload drives concurrent friend-request accept flows and verifies
// that a handled request disappears from the recipient's inbox and that the
// friend relation is visible immediately.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/internal/component/token"
)

type envelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Requests []struct {
			FromID int64 `json:"from_id"`
			Status int32 `json:"status"`
		} `json:"requests"`
		Friends []struct {
			FriendID int64 `json:"friend_id"`
		} `json:"friends"`
	} `json:"data"`
}

type pairResult struct {
	latency time.Duration
	err     error
}

func main() {
	api := flag.String("api", "http://127.0.0.1:8080", "ZIM API base URL")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	users := flag.Int("pairs", 50, "friend pairs to exercise")
	concurrency := flag.Int("concurrency", 10, "parallel pairs")
	uidBase := flag.Int64("uid-base", 980000, "first synthetic uid")
	flag.Parse()
	if *users <= 0 || *concurrency <= 0 {
		fail("pairs and concurrency must be positive")
	}
	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		fail("issuer: %v", err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	sem := make(chan struct{}, *concurrency)
	results := make(chan pairResult, *users)
	var wg sync.WaitGroup
	started := time.Now()
	for i := 0; i < *users; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }()
			start := time.Now()
			err := runPair(client, issuer, *api, *uidBase+int64(index*2), *uidBase+int64(index*2+1))
			results <- pairResult{latency: time.Since(start), err: err}
		}(i)
	}
	wg.Wait()
	close(results)

	latencies := make([]time.Duration, 0, *users)
	var failures atomic.Int64
	var firstErr atomic.Pointer[string]
	for result := range results {
		if result.err != nil {
			failures.Add(1)
			if firstErr.Load() == nil {
				value := result.err.Error()
				firstErr.CompareAndSwap(nil, &value)
			}
			continue
		}
		latencies = append(latencies, result.latency)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("pairs=%d success=%d failures=%d elapsed=%s\n",
		*users, len(latencies), failures.Load(), time.Since(started).Round(time.Millisecond))
	if len(latencies) > 0 {
		fmt.Printf("pair p50=%s p95=%s p99=%s max=%s\n",
			pct(latencies, 0.50), pct(latencies, 0.95), pct(latencies, 0.99),
			latencies[len(latencies)-1].Round(time.Millisecond))
	}
	if firstErr.Load() != nil {
		fmt.Printf("first failure: %s\n", *firstErr.Load())
	}
	if failures.Load() > 0 {
		os.Exit(1)
	}
}

func runPair(client *http.Client, issuer *token.Issuer, api string, a, b int64) error {
	tokenA, err := issue(issuer, a)
	if err != nil {
		return err
	}
	tokenB, err := issue(issuer, b)
	if err != nil {
		return err
	}
	if err = call(client, http.MethodPost, api+"/v1/friends", tokenA,
		map[string]any{"friend_id": b, "why": "load"}); err != nil {
		return fmt.Errorf("add: %w", err)
	}
	inbox, err := requests(client, api, tokenB)
	if err != nil {
		return fmt.Errorf("inbox before: %w", err)
	}
	if len(inbox.Data.Requests) != 1 || inbox.Data.Requests[0].FromID != a || inbox.Data.Requests[0].Status != 0 {
		return fmt.Errorf("inbox before = %+v", inbox.Data.Requests)
	}
	if err = call(client, http.MethodPost, api+"/v1/friend-requests/answer", tokenB,
		map[string]any{"friend_id": a, "accept": true}); err != nil {
		return fmt.Errorf("answer: %w", err)
	}
	// Retrying accept after a lost response must stay idempotent.
	if err = call(client, http.MethodPost, api+"/v1/friend-requests/answer", tokenB,
		map[string]any{"friend_id": a, "accept": true}); err != nil {
		return fmt.Errorf("answer retry: %w", err)
	}
	inbox, err = requests(client, api, tokenB)
	if err != nil {
		return fmt.Errorf("inbox after: %w", err)
	}
	if len(inbox.Data.Requests) != 0 {
		return fmt.Errorf("handled request still in inbox: %+v", inbox.Data.Requests)
	}
	friends, err := friends(client, api, tokenB)
	if err != nil {
		return fmt.Errorf("friends: %w", err)
	}
	found := false
	for _, item := range friends.Data.Friends {
		if item.FriendID == a {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("friend %d not visible", a)
	}
	// Keep the local DB bounded between load rounds.
	_ = call(client, http.MethodDelete, fmt.Sprintf("%s/v1/friends/%d", api, b), tokenA, nil)
	return nil
}

func issue(issuer *token.Issuer, uid int64) (string, error) {
	pair, err := issuer.Issue(uid, 4, "friendload")
	if err != nil {
		return "", err
	}
	return pair.AccessToken, nil
}

func requests(client *http.Client, api, bearer string) (*envelope, error) {
	return do(client, http.MethodGet, api+"/v1/friend-requests?type=in", bearer, nil)
}

func friends(client *http.Client, api, bearer string) (*envelope, error) {
	return do(client, http.MethodGet, api+"/v1/friends", bearer, nil)
}

func call(client *http.Client, method, url, bearer string, body any) error {
	response, err := do(client, method, url, bearer, body)
	if err != nil {
		return err
	}
	if response.Code != 0 {
		return fmt.Errorf("code=%d message=%s", response.Code, response.Message)
	}
	return nil
}

func do(client *http.Client, method, url, bearer string, body any) (*envelope, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	payload := &envelope{}
	if err = json.Unmarshal(raw, payload); err != nil {
		return nil, fmt.Errorf("decode status=%d: %w", response.StatusCode, err)
	}
	if response.StatusCode >= 400 {
		return payload, fmt.Errorf("status=%d code=%d", response.StatusCode, payload.Code)
	}
	return payload, nil
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

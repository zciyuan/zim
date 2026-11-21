// Command webhookdlq replays failed webhook events from PostgreSQL.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := flag.String("dsn", "postgres://zim:zim@127.0.0.1:15432/zim?sslmode=disable", "PostgreSQL DSN")
	url := flag.String("url", "", "webhook URL")
	secret := flag.String("secret", "", "HMAC secret")
	limit := flag.Int("limit", 20, "max events")
	flag.Parse()
	if *url == "" {
		panic("url is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, `SELECT id, payload FROM zim_webhook_dlq ORDER BY id LIMIT $1`, *limit)
	if err != nil {
		panic(err)
	}
	type item struct {
		id      int64
		payload []byte
	}
	items := make([]item, 0, *limit)
	for rows.Next() {
		var entry item
		if err = rows.Scan(&entry.id, &entry.payload); err != nil {
			panic(err)
		}
		items = append(items, entry)
	}
	rows.Close()

	for _, entry := range items {
		if err = post(*url, *secret, entry.payload); err != nil {
			fmt.Printf("id=%d failed: %v\n", entry.id, err)
			continue
		}
		_, _ = pool.Exec(ctx, `DELETE FROM zim_webhook_dlq WHERE id = $1`, entry.id)
		fmt.Printf("id=%d replayed\n", entry.id)
	}
}

func post(url, secret string, body []byte) error {
	request, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-ZIM-Timestamp", timestamp)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write(body)
	request.Header.Set("X-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("status %d", response.StatusCode)
	}
	return nil
}

// Command ticketprobe verifies one-time ws-ticket issuance and gate consumption.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/v2/encoding/json"
)

func main() {
	api := flag.String("api", "http://127.0.0.1:8080", "ZIM API base URL")
	gate := flag.String("gate", "ws://127.0.0.1:3553/ws", "gate WebSocket URL")
	username := flag.String("username", "ticketuser", "account")
	password := flag.String("password", "Passw0rd!123", "password")
	flag.Parse()

	token, err := login(*api, *username, *password)
	if err != nil {
		fail("login: %v", err)
	}
	ticket, err := issueTicket(*api, token)
	if err != nil {
		fail("ticket: %v", err)
	}
	fmt.Println("ticket issued")

	first := ws.NewClient(ws.WithClientUrl(fmt.Sprintf("%s?ticket=%s&platform=2&sdk=ticketprobe", *gate, ticket)))
	conn, err := first.Dial()
	if err != nil {
		fail("first dial: %v", err)
	}
	_ = conn.Close()
	fmt.Println("first dial ok")

	second := ws.NewClient(ws.WithClientUrl(fmt.Sprintf("%s?ticket=%s&platform=2&sdk=ticketprobe", *gate, ticket)))
	if _, err = second.Dial(); err == nil {
		fail("second dial unexpectedly succeeded")
	}
	fmt.Println("second dial rejected (one-time ticket)")
}

func login(api, username, password string) (string, error) {
	body, _ := json.Marshal(map[string]any{"username": username, "password": password, "platform": 2})
	response, err := http.Post(api+"/v1/auth/token", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()
	var payload struct {
		Data struct {
			AccessToken string `json:"access_token"`
		} `json:"data"`
	}
	responseBody, _ := io.ReadAll(response.Body)
	_ = json.Unmarshal(responseBody, &payload)
	if payload.Data.AccessToken == "" {
		return "", fmt.Errorf("status %d", response.StatusCode)
	}
	return payload.Data.AccessToken, nil
}

func issueTicket(api, token string) (string, error) {
	request, _ := http.NewRequest(http.MethodGet, api+"/v1/ws/ticket?platform=2", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()
	var payload struct {
		Data struct {
			Ticket string `json:"ticket"`
		} `json:"data"`
	}
	responseBody, _ := io.ReadAll(response.Body)
	_ = json.Unmarshal(responseBody, &payload)
	if payload.Data.Ticket == "" {
		return "", fmt.Errorf("status %d", response.StatusCode)
	}
	return payload.Data.Ticket, nil
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

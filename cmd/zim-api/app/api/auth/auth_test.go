package auth

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	duehttp "github.com/dobyte/due/component/http/v2"
	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/transport"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authpb "github.com/zciyuan/ZIM/internal/service/auth/pb"
)

type fakeAuthClient struct {
	loginErr error
}

func (f *fakeAuthClient) VerifyToken(context.Context, *authpb.VerifyTokenArgs, ...grpc.CallOption) (*authpb.VerifyTokenReply, error) {
	return &authpb.VerifyTokenReply{}, nil
}

func (f *fakeAuthClient) Login(_ context.Context, args *authpb.LoginArgs, _ ...grpc.CallOption) (*authpb.TokenPair, error) {
	if f.loginErr != nil {
		return nil, f.loginErr
	}
	return &authpb.TokenPair{Uid: 10001, AccessToken: "access", RefreshToken: "refresh"}, nil
}

func (f *fakeAuthClient) Register(context.Context, *authpb.RegisterArgs, ...grpc.CallOption) (*authpb.TokenPair, error) {
	return &authpb.TokenPair{Uid: 10002, AccessToken: "access2"}, nil
}

func (f *fakeAuthClient) Refresh(context.Context, *authpb.RefreshArgs, ...grpc.CallOption) (*authpb.TokenPair, error) {
	return &authpb.TokenPair{Uid: 10001, AccessToken: "access3"}, nil
}

func withFake(client *fakeAuthClient) *API {
	api := NewAPI(nil)
	api.clientFactory = func(transport.NewMeshClient) (authpb.AuthClient, error) { return client, nil }
	return api
}

func TestTokenEndpoint(t *testing.T) {
	addr := freeAddr(t)
	server := duehttp.NewServer(duehttp.WithAddr(addr))
	app := withFake(&fakeAuthClient{})
	server.Proxy().Router().Post("/v1/auth/token", app.Token)
	server.Start()
	defer server.Destroy()
	waitReady(t, addr)

	statusCode, body := postJSON(t, "http://"+addr+"/v1/auth/token", `{"username":"alice","password":"password","platform":2}`)
	if statusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", statusCode, body)
	}
	var res response
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.Code != 0 || res.RequestID == "" {
		t.Fatalf("res = %+v", res)
	}
	data, ok := res.Data.(map[string]any)
	if !ok || data["uid"] != "10001" || data["access_token"] != "access" {
		t.Fatalf("data = %#v", res.Data)
	}
}

func TestTokenEndpointRejectsWrongPassword(t *testing.T) {
	addr := freeAddr(t)
	server := duehttp.NewServer(duehttp.WithAddr(addr))
	app := withFake(&fakeAuthClient{loginErr: status.Error(grpccodes.Unauthenticated, "bad")})
	server.Proxy().Router().Post("/v1/auth/token", app.Token)
	server.Start()
	defer server.Destroy()
	waitReady(t, addr)

	statusCode, body := postJSON(t, "http://"+addr+"/v1/auth/token", `{"username":"alice","password":"bad","platform":2}`)
	if statusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, body = %s", statusCode, body)
	}
	var failure response
	if err := json.Unmarshal(body, &failure); err != nil {
		t.Fatalf("decode failure: %v", err)
	}
	if failure.Code != 100001 {
		t.Fatalf("failure code = %d", failure.Code)
	}
}

func TestTokenEndpointWithoutClient(t *testing.T) {
	addr := freeAddr(t)
	server := duehttp.NewServer(duehttp.WithAddr(addr))
	app := NewAPI(nil)
	server.Proxy().Router().Post("/v1/auth/token", app.Token)
	server.Start()
	defer server.Destroy()
	waitReady(t, addr)

	statusCode, body := postJSON(t, "http://"+addr+"/v1/auth/token", `{"username":"alice","password":"password","platform":2}`)
	if statusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", statusCode, body)
	}
}
func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server %s did not become ready", addr)
}

func postJSON(t *testing.T, url, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", "req_test")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = res.Body.Close() }()
	var buf bytes.Buffer
	if _, err = buf.ReadFrom(res.Body); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Logf("response: %s", buf.String())
	}
	return res.StatusCode, buf.Bytes()
}

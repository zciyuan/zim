package client

import (
	"context"
	"errors"
	"testing"

	"github.com/dobyte/due/v2/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zciyuan/ZIM/internal/define"
)

func (f *fakeTransportClient) Call(context.Context, string, string, any, any, ...any) error {
	return nil
}

func (f *fakeTransportClient) Client() any {
	return f.conn
}

func TestNewClientSuccess(t *testing.T) {
	conn, err := grpc.NewClient("passthrough:///zim-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	client, err := NewClient(func(target string) (transport.Client, error) {
		if target != define.AuthTarget {
			t.Fatalf("target = %q", target)
		}
		return &fakeTransportClient{conn: conn}, nil
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if client == nil {
		t.Fatal("nil client")
	}
}

func TestNewClientError(t *testing.T) {
	want := errors.New("registry down")
	if _, err := NewClient(func(string) (transport.Client, error) {
		return nil, want
	}); !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}

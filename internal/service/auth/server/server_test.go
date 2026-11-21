package server

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/internal/component/token"
	pb "github.com/zciyuan/ZIM/internal/service/auth/pb"
)

func TestVerifyToken(t *testing.T) {
	issuer, err := token.New("0123456789abcdef", time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithIssuer(nil, issuer)
	pair, err := issuer.Issue(10001, 2, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}

	reply, err := server.VerifyToken(context.Background(), &pb.VerifyTokenArgs{
		Token: pair.AccessToken, Platform: 2,
	})
	if err != nil {
		t.Fatalf("VerifyToken: %v", err)
	}
	if reply.GetUid() != 10001 || reply.GetPlatform() != 2 || reply.GetExpiresAt() == 0 {
		t.Fatalf("reply = %+v", reply)
	}

	cases := []*pb.VerifyTokenArgs{
		nil,
		{},
		{Token: "bad", Platform: 2},
		{Token: pair.AccessToken, Platform: 1},
	}
	for _, args := range cases {
		if _, err = server.VerifyToken(context.Background(), args); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("args %+v err = %v", args, err)
		}
	}
}

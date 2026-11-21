package logic

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zciyuan/ZIM/internal/component/token"
	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	authpb "github.com/zciyuan/ZIM/internal/service/auth/pb"
	authserver "github.com/zciyuan/ZIM/internal/service/auth/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func TestLoginWithIssuedToken(t *testing.T) {
	issuer, err := token.New("0123456789abcdef", time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := issuer.Issue(10001, 2, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	mesh := authserver.NewServerWithIssuer(nil, issuer)
	var bound bool
	service := newLoginService(
		func(ctx context.Context, args *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) {
			return mesh.VerifyToken(ctx, args)
		},
		func(context.Context, int64, int32, int32) error {
			bound = true
			return nil
		},
	)

	res, err := service.login(context.Background(), &authwire.LoginReq{
		Token: pair.AccessToken, Platform: 2, Sdk: "1.0.0",
	})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if res.GetUid() != "10001" || !bound {
		t.Fatalf("res = %+v, bound = %v", res, bound)
	}
}

func TestLoginSuccess(t *testing.T) {
	var gotArgs *authpb.VerifyTokenArgs
	var boundUID int64
	var boundPlatform int32
	now := time.Unix(1_700_000_000, 0)
	service := newLoginServiceWithClock(
		func(_ context.Context, args *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) {
			gotArgs = args
			return &authpb.VerifyTokenReply{Uid: 10001, Platform: 2}, nil
		},
		func(_ context.Context, uid int64, platform int32, _ int32) error {
			boundUID, boundPlatform = uid, platform
			return nil
		},
		func() time.Time { return now },
	)

	res, err := service.login(context.Background(), &authwire.LoginReq{
		Token: "t", Platform: 2, Sdk: "1.0.0", DeviceName: "Pixel",
	})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if res.GetUid() != "10001" || res.GetServerTime() != now.UnixMilli() {
		t.Fatalf("res = %+v", res)
	}
	if len(res.GetOnlineDevices()) != 1 || res.GetOnlineDevices()[0].GetDeviceName() != "Pixel" {
		t.Fatalf("devices = %+v", res.GetOnlineDevices())
	}
	if boundUID != 10001 || boundPlatform != 2 {
		t.Fatalf("bound = %d/%d", boundUID, boundPlatform)
	}
	if gotArgs.GetSdk() != "1.0.0" || gotArgs.GetToken() != "t" {
		t.Fatalf("args = %+v", gotArgs)
	}
}

func TestLoginErrors(t *testing.T) {
	valid := &authpb.VerifyTokenReply{Uid: 1, Platform: 1}
	cases := []struct {
		name    string
		req     *authwire.LoginReq
		verify  verifyTokenFunc
		bindErr error
		want    error
	}{
		{name: "nil request", req: nil, verify: func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) { return valid, nil }, want: errs.ErrUnauthorized},
		{name: "empty token", req: &authwire.LoginReq{Platform: 1}, verify: func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) { return valid, nil }, want: errs.ErrUnauthorized},
		{name: "bad platform", req: &authwire.LoginReq{Token: "t", Platform: 99}, verify: func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) { return valid, nil }, want: errs.ErrUnauthorized},
		{name: "verify error", req: &authwire.LoginReq{Token: "t", Platform: 1}, verify: func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) {
			return nil, errors.New("bad token")
		}, want: errs.ErrUnauthorized},
		{name: "nil reply", req: &authwire.LoginReq{Token: "t", Platform: 1}, verify: func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) { return nil, nil }, want: errs.ErrUnauthorized},
		{name: "platform mismatch", req: &authwire.LoginReq{Token: "t", Platform: 1}, verify: func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) {
			return &authpb.VerifyTokenReply{Uid: 1, Platform: 2}, nil
		}, want: errs.ErrUnauthorized},
		{name: "bind error", req: &authwire.LoginReq{Token: "t", Platform: 1}, verify: func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) { return valid, nil }, bindErr: errors.New("locator down"), want: errs.ErrInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service := newLoginService(tc.verify, func(context.Context, int64, int32, int32) error { return tc.bindErr })
			_, err := service.login(context.Background(), tc.req)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

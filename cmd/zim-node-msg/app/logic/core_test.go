package logic

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	authpb "github.com/zciyuan/ZIM/internal/service/auth/pb"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func (f *fakeNodeContext) Parse(v any) error {
	if f.parseErr != nil {
		return f.parseErr
	}
	target, ok := v.(*contract.Envelope)
	if !ok {
		return errors.New("unexpected parse target")
	}
	target.Opcode = f.envelope.Opcode
	target.Payload = append([]byte(nil), f.envelope.Payload...)
	return nil
}

func (f *fakeNodeContext) Response(v any) error {
	f.response = v.(*contract.Envelope)
	return f.responseErr
}

func (f *fakeNodeContext) BindGate(...int64) error { return nil }

func (f *fakeNodeContext) Context() context.Context { return context.Background() }
func (f *fakeNodeContext) CID() int64               { return 1 }
func (f *fakeNodeContext) GID() string              { return "gate-1" }
func (f *fakeNodeContext) Route() int32             { return contract.OpLogin }

func newTestCore(verify verifyTokenFunc) *core {
	return &core{
		verify: verify,
		bind: func(context.Context, loginContext, int64, int32, int32) error {
			return nil
		},
	}
}

func loginEnvelope(t *testing.T) *contract.Envelope {
	t.Helper()
	payload, err := proto.Marshal(&authwire.LoginReq{Token: "t", Platform: 1, DeviceName: "Pixel"})
	if err != nil {
		t.Fatal(err)
	}
	return &contract.Envelope{Opcode: contract.OpLogin, Payload: payload}
}

func TestDoLoginSuccess(t *testing.T) {
	ctx := &fakeNodeContext{envelope: loginEnvelope(t)}
	c := newTestCore(func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) {
		return &authpb.VerifyTokenReply{Uid: 10001, Platform: 1}, nil
	})
	if err := c.doLogin(ctx); err != nil {
		t.Fatalf("doLogin: %v", err)
	}
	if ctx.response == nil || ctx.response.GetCode() != 0 {
		t.Fatalf("response = %+v", ctx.response)
	}
	res := &authwire.LoginRes{}
	if err := proto.Unmarshal(ctx.response.GetPayload(), res); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if res.GetUid() != "10001" {
		t.Fatalf("uid = %q", res.GetUid())
	}

}

func TestDoLoginParseError(t *testing.T) {
	ctx := &fakeNodeContext{parseErr: errors.New("bad protobuf")}
	c := newTestCore(nil)
	if err := c.doLogin(ctx); err == nil {
		t.Fatal("expected parse error")
	}
	if ctx.response == nil || ctx.response.GetCode() != int32(errs.ErrInternal.Code().Code()) {
		t.Fatalf("response = %+v", ctx.response)
	}
}

func TestDoLoginPayloadError(t *testing.T) {
	ctx := &fakeNodeContext{envelope: &contract.Envelope{Opcode: contract.OpLogin, Payload: []byte{0xff}}}
	c := newTestCore(nil)
	if err := c.doLogin(ctx); err == nil {
		t.Fatal("expected payload error")
	}
}

func TestDoLoginServiceError(t *testing.T) {
	ctx := &fakeNodeContext{envelope: loginEnvelope(t)}
	c := newTestCore(func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) {
		return nil, errors.New("unauthorized")
	})
	if err := c.doLogin(ctx); err == nil {
		t.Fatal("expected login error")
	}
	if ctx.response == nil || ctx.response.GetCode() != 100002 {
		t.Fatalf("response = %+v", ctx.response)
	}
}

func TestDoLoginResponseError(t *testing.T) {
	ctx := &fakeNodeContext{envelope: loginEnvelope(t), responseErr: errors.New("closed")}
	c := newTestCore(func(context.Context, *authpb.VerifyTokenArgs) (*authpb.VerifyTokenReply, error) {
		return &authpb.VerifyTokenReply{Uid: 1, Platform: 1}, nil
	})
	if err := c.doLogin(ctx); err != nil {
		t.Fatalf("doLogin: %v", err)
	}
}

// Package client is the typed mesh client for the voiceroom service. It
// converts gRPC status errors back into zimfw/contract/errs business errors:
// the server returns errs errors whose "%d: message" text travels in the
// gRPC status message, so the caller can keep using errs.IsCode/errs.From.
package client

import (
	"context"
	"strconv"
	"strings"

	"github.com/dobyte/due/v2/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/internal/define"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// Client is the typed voiceroom mesh client.
type Client struct {
	inner pb.VoiceroomClient
}

// NewClient creates a voiceroom mesh client.
func NewClient(fn transport.NewMeshClient) (*Client, error) {
	conn, err := fn(define.VoiceroomTarget)
	if err != nil {
		return nil, err
	}
	return &Client{inner: pb.NewVoiceroomClient(conn.Client().(grpc.ClientConnInterface))}, nil
}

// FromStatus maps a gRPC error back to a business error: "180102: seat
// occupied" becomes the registered errs error. Unknown codes fall through to
// the original error so errs.From still wraps them as internal errors.
func FromStatus(err error) error {
	if err == nil {
		return nil
	}
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return err
	}
	code, message, found := parseZimCode(grpcStatus.Message())
	if !found {
		if grpcStatus.Code() == codes.DeadlineExceeded {
			return errs.ErrInternal.WithCause(err)
		}
		return err
	}
	return errs.New(code, message, 0)
}

func parseZimCode(message string) (int, string, bool) {
	index := strings.IndexByte(message, ':')
	if index <= 0 {
		return 0, "", false
	}
	code, err := strconv.Atoi(message[:index])
	if err != nil || code < 100000 || code > 999999 {
		return 0, "", false
	}
	return code, strings.TrimSpace(message[index+1:]), true
}

// CreateRoom invokes the RPC.
func (c *Client) CreateRoom(ctx context.Context, args *pb.CreateRoomArgs, opts ...grpc.CallOption) (*pb.CreateRoomReply, error) {
	reply, err := c.inner.CreateRoom(ctx, args, opts...)
	return reply, FromStatus(err)
}

// ListRooms invokes the RPC.
func (c *Client) ListRooms(ctx context.Context, args *pb.ListRoomsArgs, opts ...grpc.CallOption) (*pb.ListRoomsReply, error) {
	reply, err := c.inner.ListRooms(ctx, args, opts...)
	return reply, FromStatus(err)
}

// JoinRoom invokes the RPC.
func (c *Client) JoinRoom(ctx context.Context, args *pb.JoinRoomArgs, opts ...grpc.CallOption) (*pb.JoinRoomReply, error) {
	reply, err := c.inner.JoinRoom(ctx, args, opts...)
	return reply, FromStatus(err)
}

// RefreshToken invokes the RPC.
func (c *Client) RefreshToken(ctx context.Context, args *pb.RefreshTokenArgs, opts ...grpc.CallOption) (*pb.RefreshTokenReply, error) {
	reply, err := c.inner.RefreshToken(ctx, args, opts...)
	return reply, FromStatus(err)
}

// TakeSeat invokes the RPC.
func (c *Client) TakeSeat(ctx context.Context, args *pb.TakeSeatArgs, opts ...grpc.CallOption) (*pb.SeatOperationReply, error) {
	reply, err := c.inner.TakeSeat(ctx, args, opts...)
	return reply, FromStatus(err)
}

// LeaveSeat invokes the RPC.
func (c *Client) LeaveSeat(ctx context.Context, args *pb.LeaveSeatArgs, opts ...grpc.CallOption) (*pb.SeatOperationReply, error) {
	reply, err := c.inner.LeaveSeat(ctx, args, opts...)
	return reply, FromStatus(err)
}

// KickSeat invokes the RPC.
func (c *Client) KickSeat(ctx context.Context, args *pb.KickSeatArgs, opts ...grpc.CallOption) (*pb.SeatOperationReply, error) {
	reply, err := c.inner.KickSeat(ctx, args, opts...)
	return reply, FromStatus(err)
}

// ForceMuteSeat invokes the RPC.
func (c *Client) ForceMuteSeat(ctx context.Context, args *pb.ForceMuteSeatArgs, opts ...grpc.CallOption) (*pb.SeatOperationReply, error) {
	reply, err := c.inner.ForceMuteSeat(ctx, args, opts...)
	return reply, FromStatus(err)
}

// LockSeat invokes the RPC.
func (c *Client) LockSeat(ctx context.Context, args *pb.LockSeatArgs, opts ...grpc.CallOption) (*pb.SeatOperationReply, error) {
	reply, err := c.inner.LockSeat(ctx, args, opts...)
	return reply, FromStatus(err)
}

// HandUp invokes the RPC.
func (c *Client) HandUp(ctx context.Context, args *pb.HandUpArgs, opts ...grpc.CallOption) (*pb.SeatOperationReply, error) {
	reply, err := c.inner.HandUp(ctx, args, opts...)
	return reply, FromStatus(err)
}

// ApproveHandUp invokes the RPC.
func (c *Client) ApproveHandUp(ctx context.Context, args *pb.ApproveHandUpArgs, opts ...grpc.CallOption) (*pb.SeatOperationReply, error) {
	reply, err := c.inner.ApproveHandUp(ctx, args, opts...)
	return reply, FromStatus(err)
}

// InviteToSeat invokes the RPC.
func (c *Client) InviteToSeat(ctx context.Context, args *pb.InviteToSeatArgs, opts ...grpc.CallOption) (*pb.SeatOperationReply, error) {
	reply, err := c.inner.InviteToSeat(ctx, args, opts...)
	return reply, FromStatus(err)
}

// GetRoom invokes the RPC.
func (c *Client) GetRoom(ctx context.Context, args *pb.GetRoomArgs, opts ...grpc.CallOption) (*pb.RoomInfo, error) {
	reply, err := c.inner.GetRoom(ctx, args, opts...)
	return reply, FromStatus(err)
}

// CloseRoom invokes the RPC.
func (c *Client) CloseRoom(ctx context.Context, args *pb.CloseRoomArgs, opts ...grpc.CallOption) (*pb.SeatOperationReply, error) {
	reply, err := c.inner.CloseRoom(ctx, args, opts...)
	return reply, FromStatus(err)
}

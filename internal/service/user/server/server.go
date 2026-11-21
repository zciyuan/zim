package server

import (
	"context"
	"strings"
	"time"

	"github.com/dobyte/due/v2/cluster/mesh"
	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/user/pb"
)

// Repository is the user data required by the service.
type Repository interface {
	FindByID(ctx context.Context, uid int64) (*model.User, error)
	FindByIDs(ctx context.Context, uids []int64) ([]*model.User, error)
	Search(ctx context.Context, keyword string, limit int) ([]*model.User, error)
	UpdateProfile(ctx context.Context, uid int64, nickname, faceURL string, gender int16, now int64) error
}

var _ pb.UserServer = (*Server)(nil)

// NewServer returns the user service.
func NewServer(proxy *mesh.Proxy, repo Repository) *Server {
	return &Server{proxy: proxy, repo: repo}
}

// Init registers the service provider under the discovery name user.
func (s *Server) Init() {
	s.proxy.AddServiceProvider(define.ServiceNameUser, &pb.User_ServiceDesc, s)
}

// GetProfile returns one user profile.
func (s *Server) GetProfile(ctx context.Context, args *pb.GetProfileArgs) (*pb.UserProfile, error) {
	user, err := s.doGetProfile(ctx, getProfileArgs{UID: args.GetUid()})
	if err != nil {
		return nil, err
	}
	return profile(user), nil
}

// BatchGetUsers returns users that exist, preserving no particular order.
func (s *Server) BatchGetUsers(ctx context.Context, args *pb.BatchGetUsersArgs) (*pb.BatchGetUsersReply, error) {
	users, err := s.doBatchGetUsers(ctx, batchGetUsersArgs{UIDs: args.GetUids()})
	if err != nil {
		return nil, err
	}
	reply := &pb.BatchGetUsersReply{Users: make([]*pb.UserProfile, 0, len(users))}
	for _, item := range users {
		reply.Users = append(reply.Users, profile(item))
	}
	return reply, nil
}

// UpdateProfile updates nickname/avatar/gender.
func (s *Server) UpdateProfile(ctx context.Context, args *pb.UpdateProfileArgs) (*pb.UserProfile, error) {
	user, err := s.doUpdateProfile(ctx, updateProfileArgs{
		UID: args.GetUid(), Nickname: args.GetNickname(), Avatar: args.GetAvatar(), Gender: args.GetGender(),
	})
	if err != nil {
		return nil, err
	}
	return profile(user), nil
}

// SearchUsers searches by username or nickname.
func (s *Server) SearchUsers(ctx context.Context, args *pb.SearchUsersArgs) (*pb.SearchUsersReply, error) {
	users, err := s.doSearchUsers(ctx, searchUsersArgs{Keyword: args.GetKeyword(), Limit: args.GetLimit()})
	if err != nil {
		return nil, err
	}
	reply := &pb.SearchUsersReply{Users: make([]*pb.UserProfile, 0, len(users))}
	for _, item := range users {
		reply.Users = append(reply.Users, profile(item))
	}
	return reply, nil
}

func (s *Server) doGetProfile(ctx context.Context, args getProfileArgs) (*model.User, error) {
	if args.UID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "uid invalid")
	}
	if user, ok := s.cachedProfile(ctx, args.UID); ok {
		return user, nil
	}
	user, err := s.repo.FindByID(ctx, args.UID)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Error(codes.Internal, "user lookup failed")
	}
	s.storeProfile(ctx, user)
	return user, nil
}

func (s *Server) doBatchGetUsers(ctx context.Context, args batchGetUsersArgs) ([]*model.User, error) {
	if len(args.UIDs) == 0 {
		return nil, nil
	}
	users := make([]*model.User, 0, len(args.UIDs))
	missing := make([]int64, 0, len(args.UIDs))
	for _, uid := range args.UIDs {
		if user, ok := s.cachedProfile(ctx, uid); ok {
			users = append(users, user)
		} else {
			missing = append(missing, uid)
		}
	}
	if len(missing) == 0 {
		return users, nil
	}
	loaded, err := s.repo.FindByIDs(ctx, missing)
	if err != nil {
		return nil, status.Error(codes.Internal, "user lookup failed")
	}
	for _, user := range loaded {
		s.storeProfile(ctx, user)
	}
	return append(users, loaded...), nil
}

func (s *Server) doUpdateProfile(ctx context.Context, args updateProfileArgs) (*model.User, error) {
	if args.UID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "uid invalid")
	}
	if len([]rune(args.Nickname)) > 32 {
		return nil, status.Error(codes.InvalidArgument, "nickname too long")
	}
	if err := s.repo.UpdateProfile(ctx, args.UID, args.Nickname, args.Avatar,
		int16(args.Gender), time.Now().UnixMilli()); err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Error(codes.Internal, "update failed")
	}
	s.invalidateProfile(ctx, args.UID)
	return s.doGetProfile(ctx, getProfileArgs{UID: args.UID})
}

func (s *Server) doSearchUsers(ctx context.Context, args searchUsersArgs) ([]*model.User, error) {
	keyword := strings.TrimSpace(args.Keyword)
	if keyword == "" {
		return nil, nil
	}
	users, err := s.repo.Search(ctx, keyword, int(args.Limit))
	if err != nil {
		log.Warnf("user search failed: %v", err)
		return nil, status.Error(codes.Internal, "search failed")
	}
	return users, nil
}

func profile(user *model.User) *pb.UserProfile {
	if user == nil {
		return nil
	}
	return &pb.UserProfile{
		Uid:       user.ID,
		Username:  user.Username,
		Nickname:  user.Nickname,
		Avatar:    user.FaceURL,
		Gender:    int32(user.Gender),
		Status:    int32(user.Status),
		UpdatedAt: user.UpdatedAt,
	}
}

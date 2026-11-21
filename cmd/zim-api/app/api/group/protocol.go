package group

import (
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
)

type readCountRes struct {
	ReadCount int `json:"read_count"`
}

type groupsRes struct {
	Groups []*pb.GroupInfo `json:"groups"`
}

type appliesRes struct {
	Applies []*pb.GroupApplyItem `json:"applies"`
}

type membersRes struct {
	Members []*pb.Membership `json:"members"`
}

type createFamilyReq struct {
	Name      string  `json:"name"`
	MemberIDs []int64 `json:"member_ids"`
}

type inviteFamilyReq struct {
	UserIDs []int64 `json:"user_ids"`
}

type operationRes struct {
	OK bool `json:"ok"`
}

type familySettingsReq struct {
	AutoAnswerEnabled bool `json:"auto_answer_enabled"`
	FamilyMode        bool `json:"family_mode"`
}

type familyConsentReq struct {
	Scope       string `json:"scope"`
	Granted     bool   `json:"granted"`
	GuardianUID int64  `json:"guardian_uid"`
	Channel     string `json:"channel"`
}

type familyAuditRes struct {
	Items []*familysvc.Audit `json:"items"`
}

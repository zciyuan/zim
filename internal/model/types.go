// Package model holds storage models shared by DAO and services.
package model

// User is one row of zim_user. Times are Unix milliseconds.
type User struct {
	ID           int64
	Username     string
	PasswordHash string
	Nickname     string
	FaceURL      string
	Gender       int16
	Status       int16
	CreatedAt    int64
	UpdatedAt    int64
}

// Friend is one directed row of zim_friend.
type Friend struct {
	UID       int64
	FriendID  int64
	Remark    string
	Pinned    int16
	CreatedAt int64
}

// FriendRequest is one row of zim_friend_request.
type FriendRequest struct {
	ID        int64
	FromID    int64
	ToID      int64
	Why       string
	Status    int16
	CreatedAt int64
	UpdatedAt int64
	HandledAt int64
}

// BlacklistEntry is one row of zim_blacklist.
type BlacklistEntry struct {
	UID       int64
	BlackID   int64
	CreatedAt int64
}

// Group is one row of zim_group.
type Group struct {
	ID                int64
	Name              string
	FaceURL           string
	Notification      string
	OwnerID           int64
	GroupType         int16
	MaxMember         int32
	NeedVerification  int16
	LookMemberInfo    int16
	ApplyMemberFriend int16
	Status            int16
	MutedUntil        int64
	CreatedAt         int64
}

// GroupMember is one row of zim_group_member.
type GroupMember struct {
	GroupID   int64
	UserID    int64
	Role      int16
	MuteUntil int64
	JoinedAt  int64
}

// GroupApply is one row of zim_group_apply.
type GroupApply struct {
	ID        int64
	GroupID   int64
	UserID    int64
	Why       string
	Status    int16
	CreatedAt int64
	HandledAt int64
}

// Conversation is one owner-scoped conversation row.
type Conversation struct {
	OwnerID      int64
	ConvID       string
	Type         int16
	PeerID       int64
	GroupID      int64
	RecvOpt      int16
	IsPinned     int16
	BurnSeconds  int32
	Deleted      int16
	LastMsgID    string
	LastMsgSeq   int64
	LastMsgAt    int64
	LastMsgBrief string
	ReadSeq      int64
	MinSeq       int64
	UpdatedAt    int64
}

// SeqUser is one user's read/min watermarks in a conversation.
type SeqUser struct {
	UID            int64
	ConversationID string
	ReadSeq        int64
	MinSeq         int64
	UpdatedAt      int64
}

// Message is one message stored in a Mongo aggregate document.
type Message struct {
	ServerMsgID    string
	ClientMsgID    string
	ConversationID string
	Seq            int64
	SendID         int64
	RecvID         int64
	GroupID        int64
	MsgType        int32
	Content        string
	AtUserIDs      []string
	SendTime       int64
	Status         int32
	// ADR-0043: burn-after-reading and business-card extension.
	BurnTTL        int32
	BurnExpireAt   int64
	MaxRetentionAt int64
	IsBurned       bool
	SecurityFlags  uint32
	ExtType        int32
	ExtPayload     string
}

// GroupSecuritySettings is one row of zim_group_security_settings (ADR-0043).
type GroupSecuritySettings struct {
	GroupID             int64
	MuteAll             bool
	ScheduledMuteEnable bool
	ScheduledMuteStart  string // HH:MM
	ScheduledMuteEnd    string // HH:MM
	ScheduledTimezone   string // IANA
	DefaultMuteFlags    int32
	NewMemberMuteSec    int32
	AntiSpamEnable      bool
	AntiSpamWindowSec   int32
	AntiSpamMaxMsgs     int32
	SensitiveAutoMuteS  int32
	AntiScreenshot      bool
	WatermarkEnable     bool
	ScreenshotPenalty   int16 // 0=WARN 1=MUTE_24H 2=KICK
	UpdatedAt           int64
}

// GroupMemberRestriction is one row of zim_group_member_restrictions (ADR-0043).
type GroupMemberRestriction struct {
	GroupID     int64
	UID         int64
	IsWhitelist bool
	MuteFlags   int32
	MuteUntil   int64 // ms; 0 = not muted, -1 = permanent
	Reason      string
	OperatorUID int64 // 0 = system auto
	UpdatedAt   int64
}

// DLPSecurityEvent is one row of zim_dlp_security_events (ADR-0043).
type DLPSecurityEvent struct {
	ID          int64
	ConvID      string
	ConvType    int16
	UID         int64
	EventType   int16
	DeviceInfo  string
	ActionTaken int16
	CreatedAt   int64
}

// ChangeLog is one append-only version-log row.
type ChangeLog struct {
	UID     int64  `bson:"uid"`
	Shard   int64  `bson:"shard"`
	Entity  string `bson:"-"`
	ObjID   string `bson:"obj_id"`
	State   string `bson:"state"`
	Version int64  `bson:"version"`
	Mtime   int64  `bson:"mtime"`
}

// GroupApply is one pending/answered join request.

// SnapshotObject is one latest state in a change-log snapshot.
type SnapshotObject struct {
	ObjID   string `bson:"obj_id"`
	State   string `bson:"state"`
	Version int64  `bson:"version"`
	Mtime   int64  `bson:"mtime"`
}

// ChangeSnapshot is the compacted state for one user/entity.
type ChangeSnapshot struct {
	UID     int64             `bson:"uid"`
	Entity  string            `bson:"-"`
	Version int64             `bson:"version"`
	Objects []*SnapshotObject `bson:"objects"`
	Mtime   int64             `bson:"mtime"`
}

// Device is one row of zim_device (ADR-0024).
type Device struct {
	OwnerID      int64
	Platform     int16
	Slot         int16
	DeviceID     string
	DeviceName   string
	PushProvider string
	PushToken    string
	VoIPToken    string
	AppVersion   string
	OSVersion    string
	Status       int16
	LastActiveAt int64
	Badge        int32
	CreatedAt    int64
	UpdatedAt    int64
}

// ReadMark is one monotonic read watermark update.
type ReadMark struct {
	UID    int64
	ConvID string
	Seq    int64
}

// VoiceRoom is one row of zim_voice_room (ADR-0043). Times are Unix
// milliseconds; status 1=LIVE 2=CLOSED.
type VoiceRoom struct {
	RoomID       int64
	RoomType     int16
	GroupID      int64
	OwnerUID     int64
	Title        string
	Announcement string
	SeatCount    int16
	JoinSeatMode int16
	PasswordHash string
	Status       int16
	CreatedAt    int64
	ClosedAt     int64
}

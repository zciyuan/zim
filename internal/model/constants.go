package model

// Social enums. Values are persisted in SMALLINT columns and are append-only.
const (
	FriendRequestPending  int16 = 0
	FriendRequestAccepted int16 = 1
	FriendRequestRejected int16 = 2
	FriendRequestExpired  int16 = 3
)

// Group types and roles.
const (
	GroupTypeNormal     int16 = 1
	GroupTypeAuthorized int16 = 2
	GroupTypeFamily     int16 = 3

	GroupRoleMember int16 = 0
	GroupRoleAdmin  int16 = 1
	GroupRoleOwner  int16 = 2
	GroupRoleChild  int16 = 3
	GroupRoleElder  int16 = 4

	MaxGroupMembers  int32 = 2000
	MaxFamilyMembers int32 = 20
)

// Conversation types.
const (
	ConversationSingle int16 = 1
	ConversationGroup  int16 = 2
)

// Message status.
const (
	MessageNormal   int32 = 0
	MessageRecalled int32 = 1
	MessageDeleted  int32 = 2
)

// Message limits.
const (
	MaxMessageBytes        = 64 * 1024
	MessageBlockSize       = 100
	RetractWindowMS  int64 = 120 * 1000
)

// Group apply status.
const (
	GroupApplyPending  int16 = 0
	GroupApplyAccepted int16 = 1
	GroupApplyRejected int16 = 2
)

// Category mute flags (ADR-0043, zim_group_member_restrictions.mute_flags /
// zim_group_security_settings.default_mute_flags bit mask).
const (
	MuteFlagAll        int32 = 1 << 0 // forbid all text/basic messages
	MuteFlagImageVideo int32 = 1 << 1 // forbid image/video
	MuteFlagLink       int32 = 1 << 2 // forbid external link / QR code
	MuteFlagCard       int32 = 1 << 3 // forbid business cards
	MuteFlagRedPacket  int32 = 1 << 4 // forbid red packet
	MuteFlagSticker    int32 = 1 << 5 // forbid custom sticker
	MuteFlagNickname   int32 = 1 << 6 // forbid nickname change
	MuteFlagReaction   int32 = 1 << 7 // forbid reactions
	MuteFlagVoiceRoom  int32 = 1 << 8 // forbid voice-room seat/speak/danmaku
)

// Screenshot penalty levels (ADR-0043).
const (
	ScreenshotPenaltyWarn    int16 = 0
	ScreenshotPenaltyMute24H int16 = 1
	ScreenshotPenaltyKick    int16 = 2
)

// Burn security flags (ADR-0043, Message.security_flags bit mask).
const (
	SecurityFlagNoForward           uint32 = 1 << 0
	SecurityFlagNoCopy              uint32 = 1 << 1
	SecurityFlagNoSaveLocal         uint32 = 1 << 2
	SecurityFlagNoSearchIndex       uint32 = 1 << 3
	SecurityFlagForceAntiScreenshot uint32 = 1 << 4
)

// BurnAfterReadingFlags are the flags forced onto every burn message.
const BurnAfterReadingFlags = SecurityFlagNoForward | SecurityFlagNoCopy |
	SecurityFlagNoSaveLocal | SecurityFlagNoSearchIndex | SecurityFlagForceAntiScreenshot

// Business card extension types (ADR-0043, Message.ext_type).
const (
	ExtTypeNone      int32 = 0
	ExtTypeRedPacket int32 = 1
	ExtTypeTip       int32 = 2
	ExtTypeVoiceRoom int32 = 3
)

// Version-log change states.
const (
	ChangeInsert = "insert"
	ChangeUpdate = "update"
	ChangeDelete = "delete"
)

// Version-log entity names.
const (
	EntityConversation = "conversation"
	EntityFriend       = "friend"
	EntityGroupMember  = "group_member"
)

// Voice room enums (ADR-0043, zim_voice_room). Values are persisted in
// SMALLINT columns and are append-only.
const (
	VoiceRoomTypeGroup    int16 = 1
	VoiceRoomTypePublic   int16 = 2
	VoiceRoomTypePassword int16 = 3

	VoiceRoomStatusLive   int16 = 1
	VoiceRoomStatusClosed int16 = 2

	VoiceRoomJoinFree   int16 = 1
	VoiceRoomJoinHandUp int16 = 2

	VoiceRoomSeatCountDefault int16 = 8
	VoiceRoomSeatCountMax     int16 = 12
)

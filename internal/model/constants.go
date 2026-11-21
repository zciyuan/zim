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

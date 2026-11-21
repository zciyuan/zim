// Package contract defines the frozen ZIM protocol contract: opcodes, frame
// encoding, envelope shape, handler signatures, and the unified error package.
package contract

// Opcode segments (DL-062). The values below are frozen; add new opcodes at
// the tail of the owning segment and register them in ZIM-02 §2.3.
const (
	// SegmentAuth is the authentication segment.
	SegmentAuth int32 = 1000
	// SegmentUser is the user profile segment.
	SegmentUser int32 = 1100
	// SegmentFriend is the friend and blacklist segment.
	SegmentFriend int32 = 1200
	// SegmentGroup is the group and family segment.
	SegmentGroup int32 = 1300
	// SegmentConversation is the conversation segment.
	SegmentConversation int32 = 1400
	// SegmentMessage is the message segment.
	SegmentMessage int32 = 1500
	// SegmentSync is the synchronization segment.
	SegmentSync int32 = 1600
	// SegmentChatroom is the chat room segment (reserved in v1).
	SegmentChatroom int32 = 1700
	// SegmentRTC is the real-time communication signalling segment.
	SegmentRTC int32 = 1800
	// SegmentPush is the server-to-client push segment.
	SegmentPush int32 = 1900
	// SegmentInfra is the error and flow-control segment.
	SegmentInfra int32 = 9000
)

// Authentication opcodes.
const (
	OpLogin        int32 = 1001
	OpLogout       int32 = 1002
	OpRefreshToken int32 = 1003
	OpKickDevice   int32 = 1004
)

// User opcodes.
const (
	OpGetProfile      int32 = 1101
	OpUpdateProfile   int32 = 1102
	OpSearchUser      int32 = 1103
	OpBatchGetUsers   int32 = 1104
	OpOnlineStatus    int32 = 1105
	OpSubscribeOnline int32 = 1106
	OpPrivacy         int32 = 1107
)

// Friend opcodes.
const (
	OpAddFriend      int32 = 1201
	OpAnswerFriend   int32 = 1202
	OpFriendRequests int32 = 1203
	OpDeleteFriend   int32 = 1204
	OpFriendList     int32 = 1205
	OpUpdateRemark   int32 = 1206
	OpPinFriend      int32 = 1207
	OpAddBlack       int32 = 1208
	OpBlackList      int32 = 1209
	OpIsFriend       int32 = 1210
)

// Group opcodes.
const (
	OpCreateGroup    int32 = 1301
	OpDismissGroup   int32 = 1302
	OpQuitGroup      int32 = 1303
	OpInviteGroup    int32 = 1304
	OpApplyJoinGroup int32 = 1305
	OpAnswerJoin     int32 = 1306
	OpKickGroup      int32 = 1307
	OpTransferGroup  int32 = 1308
	OpUpdateGroup    int32 = 1309
	OpGroupMembers   int32 = 1310
	OpSetGroupAdmin  int32 = 1311
	OpMuteMember     int32 = 1312
	OpMuteAll        int32 = 1313
	OpMyGroups       int32 = 1314
	OpSearchGroup    int32 = 1315
	OpGroupNotify    int32 = 1317
)

// Conversation opcodes.
const (
	OpConversationList   int32 = 1401
	OpPinConversation    int32 = 1402
	OpNotifySetting      int32 = 1403
	OpDeleteConversation int32 = 1404
	OpReadConversation   int32 = 1405
	OpUnread             int32 = 1406
	OpAtMe               int32 = 1407
	OpRecvOpt            int32 = 1408
	OpGlobalNotify       int32 = 1409
)

// Message opcodes.
const (
	OpSendSingle     int32 = 1501
	OpSendGroup      int32 = 1502
	OpMerge          int32 = 1503
	OpQuote          int32 = 1504
	OpReadReceipt    int32 = 1505
	OpRecall         int32 = 1506
	OpEdit           int32 = 1507
	OpDeleteMsg      int32 = 1508
	OpBurn           int32 = 1509
	OpStream         int32 = 1510
	OpPullBySeq      int32 = 1511
	OpSearchMsg      int32 = 1512
	OpSendStatus     int32 = 1513
	OpSyncMissing    int32 = 1514
	OpAddReaction    int32 = 1515
	OpRemoveReaction int32 = 1516
	OpTyping         int32 = 1517
)

// Synchronization opcodes.
const (
	OpSyncCheck        int32 = 1601
	OpSyncFriend       int32 = 1603
	OpSyncGroupMember  int32 = 1604
	OpSyncConversation int32 = 1605
	OpFullSync         int32 = 1606
	OpVersionHash      int32 = 1607
)

// RTC signalling opcodes.
const (
	OpCallInvite       int32 = 1801
	OpCallAnswer       int32 = 1802
	OpCallHangup       int32 = 1803
	OpCallCancel       int32 = 1804
	OpCallState        int32 = 1805
	OpRoomToken        int32 = 1806
	OpParticipantState int32 = 1807
)

// Server-to-client push opcodes. They must never be registered as request
// handlers and always travel with seq = 0.
const (
	OpPushMsg        int32 = 1901
	OpPushKick       int32 = 1902
	OpPushOnline     int32 = 1903
	OpPushGroupEvent int32 = 1904
	OpPushProfile    int32 = 1905
	OpPushReadSync   int32 = 1906
	OpPushTyping     int32 = 1908
	OpPushFriend     int32 = 1909
	OpPushNotify     int32 = 1907
)

// Infrastructure opcodes.
const (
	OpRateLimited int32 = 9001
	OpServerBusy  int32 = 9002
)

// Message types (DL-063). Values 1-7 and 20-22 are frozen.
const (
	MsgTypeText       int32 = 1
	MsgTypeImage      int32 = 2
	MsgTypeVoice      int32 = 3
	MsgTypeVideo      int32 = 4
	MsgTypeFile       int32 = 5
	MsgTypeLocation   int32 = 6
	MsgTypeCustom     int32 = 7
	MsgTypeVoiceCall  int32 = 20
	MsgTypeVideoCall  int32 = 21
	MsgTypeGroupVoice int32 = 22
)

var segments = [...]OpcodeSegment{
	{Domain: "auth", Base: 1000, Max: 1099},
	{Domain: "user", Base: 1100, Max: 1199},
	{Domain: "friend", Base: 1200, Max: 1299},
	{Domain: "group", Base: 1300, Max: 1399},
	{Domain: "conversation", Base: 1400, Max: 1499},
	{Domain: "message", Base: 1500, Max: 1599},
	{Domain: "sync", Base: 1600, Max: 1699},
	{Domain: "chatroom", Base: 1700, Max: 1799},
	{Domain: "rtc", Base: 1800, Max: 1899},
	{Domain: "push", Base: 1900, Max: 1999},
	{Domain: "infra", Base: 9000, Max: 9999},
}

// Segments returns a copy of all opcode segments.
func Segments() []OpcodeSegment {
	segmentsCopy := make([]OpcodeSegment, len(segments))
	copy(segmentsCopy, segments[:])
	return segmentsCopy
}

// SegmentOf returns the segment containing opcode.
func SegmentOf(opcode int32) (OpcodeSegment, bool) {
	for _, segment := range segments {
		if opcode >= segment.Base && opcode <= segment.Max {
			return segment, true
		}
	}
	return OpcodeSegment{}, false
}

// IsPush reports whether opcode is a server-to-client push opcode.
func IsPush(opcode int32) bool {
	return opcode >= OpPushMsg && opcode <= OpPushNotify
}

// IsInfra reports whether opcode is an infrastructure control frame.
func IsInfra(opcode int32) bool {
	return opcode >= SegmentInfra && opcode <= 9999
}

// IsMessageType reports whether msgType is one of the frozen values.
func IsMessageType(msgType int32) bool {
	return (msgType >= MsgTypeText && msgType <= MsgTypeCustom) ||
		(msgType >= MsgTypeVoiceCall && msgType <= MsgTypeGroupVoice)
}

// AllOpcode tests every documented opcode against its expected segment. It is
// used by contract tests and by startup validation.
var AllOpcode = map[int32]OpcodeSegment{
	OpLogin: {Domain: "auth", Base: 1000, Max: 1099}, OpLogout: {Domain: "auth", Base: 1000, Max: 1099},
	OpRefreshToken: {Domain: "auth", Base: 1000, Max: 1099}, OpKickDevice: {Domain: "auth", Base: 1000, Max: 1099},
	OpGetProfile: {Domain: "user", Base: 1100, Max: 1199}, OpUpdateProfile: {Domain: "user", Base: 1100, Max: 1199},
	OpSearchUser: {Domain: "user", Base: 1100, Max: 1199}, OpBatchGetUsers: {Domain: "user", Base: 1100, Max: 1199},
	OpOnlineStatus: {Domain: "user", Base: 1100, Max: 1199}, OpSubscribeOnline: {Domain: "user", Base: 1100, Max: 1199},
	OpPrivacy:   {Domain: "user", Base: 1100, Max: 1199},
	OpAddFriend: {Domain: "friend", Base: 1200, Max: 1299}, OpAnswerFriend: {Domain: "friend", Base: 1200, Max: 1299},
	OpFriendRequests: {Domain: "friend", Base: 1200, Max: 1299}, OpDeleteFriend: {Domain: "friend", Base: 1200, Max: 1299},
	OpFriendList: {Domain: "friend", Base: 1200, Max: 1299}, OpUpdateRemark: {Domain: "friend", Base: 1200, Max: 1299},
	OpPinFriend: {Domain: "friend", Base: 1200, Max: 1299}, OpAddBlack: {Domain: "friend", Base: 1200, Max: 1299},
	OpBlackList: {Domain: "friend", Base: 1200, Max: 1299}, OpIsFriend: {Domain: "friend", Base: 1200, Max: 1299},
	OpCreateGroup: {Domain: "group", Base: 1300, Max: 1399}, OpDismissGroup: {Domain: "group", Base: 1300, Max: 1399},
	OpQuitGroup: {Domain: "group", Base: 1300, Max: 1399}, OpInviteGroup: {Domain: "group", Base: 1300, Max: 1399},
	OpApplyJoinGroup: {Domain: "group", Base: 1300, Max: 1399}, OpAnswerJoin: {Domain: "group", Base: 1300, Max: 1399},
	OpKickGroup: {Domain: "group", Base: 1300, Max: 1399}, OpTransferGroup: {Domain: "group", Base: 1300, Max: 1399},
	OpUpdateGroup: {Domain: "group", Base: 1300, Max: 1399}, OpGroupMembers: {Domain: "group", Base: 1300, Max: 1399},
	OpSetGroupAdmin: {Domain: "group", Base: 1300, Max: 1399}, OpMuteMember: {Domain: "group", Base: 1300, Max: 1399},
	OpMuteAll: {Domain: "group", Base: 1300, Max: 1399}, OpMyGroups: {Domain: "group", Base: 1300, Max: 1399},
	OpSearchGroup: {Domain: "group", Base: 1300, Max: 1399}, OpGroupNotify: {Domain: "group", Base: 1300, Max: 1399},
	OpConversationList: {Domain: "conversation", Base: 1400, Max: 1499}, OpPinConversation: {Domain: "conversation", Base: 1400, Max: 1499},
	OpNotifySetting: {Domain: "conversation", Base: 1400, Max: 1499}, OpDeleteConversation: {Domain: "conversation", Base: 1400, Max: 1499},
	OpReadConversation: {Domain: "conversation", Base: 1400, Max: 1499}, OpUnread: {Domain: "conversation", Base: 1400, Max: 1499},
	OpAtMe: {Domain: "conversation", Base: 1400, Max: 1499}, OpRecvOpt: {Domain: "conversation", Base: 1400, Max: 1499},
	OpGlobalNotify: {Domain: "conversation", Base: 1400, Max: 1499},
	OpSendSingle:   {Domain: "message", Base: 1500, Max: 1599}, OpSendGroup: {Domain: "message", Base: 1500, Max: 1599},
	OpMerge: {Domain: "message", Base: 1500, Max: 1599}, OpQuote: {Domain: "message", Base: 1500, Max: 1599},
	OpReadReceipt: {Domain: "message", Base: 1500, Max: 1599}, OpRecall: {Domain: "message", Base: 1500, Max: 1599}, OpAddReaction: {Domain: "message", Base: 1500, Max: 1599}, OpRemoveReaction: {Domain: "message", Base: 1500, Max: 1599},
	OpEdit: {Domain: "message", Base: 1500, Max: 1599}, OpDeleteMsg: {Domain: "message", Base: 1500, Max: 1599},
	OpBurn: {Domain: "message", Base: 1500, Max: 1599}, OpStream: {Domain: "message", Base: 1500, Max: 1599},
	OpPullBySeq: {Domain: "message", Base: 1500, Max: 1599}, OpSearchMsg: {Domain: "message", Base: 1500, Max: 1599},
	OpSendStatus: {Domain: "message", Base: 1500, Max: 1599}, OpSyncMissing: {Domain: "message", Base: 1500, Max: 1599},
	OpSyncCheck: {Domain: "sync", Base: 1600, Max: 1699}, OpSyncFriend: {Domain: "sync", Base: 1600, Max: 1699},
	OpSyncGroupMember: {Domain: "sync", Base: 1600, Max: 1699}, OpSyncConversation: {Domain: "sync", Base: 1600, Max: 1699},
	OpFullSync: {Domain: "sync", Base: 1600, Max: 1699}, OpVersionHash: {Domain: "sync", Base: 1600, Max: 1699},
	OpCallInvite: {Domain: "rtc", Base: 1800, Max: 1899}, OpCallAnswer: {Domain: "rtc", Base: 1800, Max: 1899},
	OpCallHangup: {Domain: "rtc", Base: 1800, Max: 1899}, OpCallCancel: {Domain: "rtc", Base: 1800, Max: 1899},
	OpCallState: {Domain: "rtc", Base: 1800, Max: 1899}, OpRoomToken: {Domain: "rtc", Base: 1800, Max: 1899},
	OpParticipantState: {Domain: "rtc", Base: 1800, Max: 1899},
	OpPushMsg:          {Domain: "push", Base: 1900, Max: 1999}, OpPushKick: {Domain: "push", Base: 1900, Max: 1999},
	OpPushOnline: {Domain: "push", Base: 1900, Max: 1999}, OpPushGroupEvent: {Domain: "push", Base: 1900, Max: 1999},
	OpPushProfile: {Domain: "push", Base: 1900, Max: 1999}, OpPushReadSync: {Domain: "push", Base: 1900, Max: 1999}, OpPushTyping: {Domain: "push", Base: 1900, Max: 1999}, OpPushFriend: {Domain: "push", Base: 1900, Max: 1999},
	OpPushNotify:  {Domain: "push", Base: 1900, Max: 1999},
	OpRateLimited: {Domain: "infra", Base: 9000, Max: 9999}, OpServerBusy: {Domain: "infra", Base: 9000, Max: 9999},
}

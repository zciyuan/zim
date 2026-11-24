package errs

import "net/http"

// CodeOK is the success code.
const CodeOK = 0

// Authentication errors (100xxx).
var (
	// ErrPasswordIncorrect indicates a wrong username or password.
	ErrPasswordIncorrect = New(100001, "username or password incorrect", http.StatusUnauthorized)
	// ErrUnauthorized indicates an invalid or expired token.
	ErrUnauthorized = New(100002, "token invalid", http.StatusUnauthorized)
	// ErrTokenKicked indicates the token was kicked by another login.
	ErrTokenKicked = New(100003, "token kicked", http.StatusUnauthorized)
	// ErrAccountDisabled indicates a disabled account.
	ErrAccountDisabled = New(100004, "account disabled", http.StatusForbidden)
	// ErrMultiDeviceConflict indicates the device limit was exceeded.
	ErrMultiDeviceConflict = New(100005, "too many devices", http.StatusConflict)
	// ErrInvalidCredentials is an alias used by REST login handlers.
	ErrInvalidCredentials = ErrPasswordIncorrect
)

// User errors (110xxx).
var (
	// ErrUserNotExist indicates the user does not exist.
	ErrUserNotExist = New(110001, "user not exist", http.StatusNotFound)
	// ErrNicknameInvalid indicates an invalid nickname.
	ErrNicknameInvalid = New(110002, "nickname invalid", http.StatusBadRequest)
	// ErrUsernameInvalid indicates a malformed username (registration).
	ErrUsernameInvalid = New(110003, "username invalid", http.StatusBadRequest)
	// ErrPasswordInvalid indicates a weak or malformed password (registration).
	ErrPasswordInvalid = New(110004, "password invalid", http.StatusBadRequest)
	// ErrUsernameTaken indicates the username is already registered.
	ErrUsernameTaken = New(110005, "username already taken", http.StatusConflict)
)

// Friend errors (120xxx).
var (
	// ErrAlreadyFriend indicates the users are already friends.
	ErrAlreadyFriend = New(120001, "already friend", http.StatusConflict)
	// ErrFriendRequestNotExist indicates a missing friend request.
	ErrFriendRequestNotExist = New(120002, "friend request not exist", http.StatusNotFound)
	// ErrBlockedByPeer indicates the peer has blocked the caller.
	ErrBlockedByPeer = New(120003, "blocked by peer", http.StatusForbidden)
)

// Group errors (130xxx).
var (
	// ErrGroupNotExist indicates the group does not exist.
	ErrGroupNotExist = New(130001, "group not exist", http.StatusNotFound)
	// ErrNotGroupMember indicates the caller is not a group member.
	ErrNotGroupMember = New(130002, "not group member", http.StatusForbidden)
	// ErrNoGroupPermission indicates the caller lacks group permission.
	ErrNoGroupPermission = New(130003, "no group permission", http.StatusForbidden)
	// ErrGroupFull indicates the group member limit was reached.
	ErrGroupFull = New(130004, "group member full", http.StatusConflict)
	// ErrOwnerCannotQuit indicates the owner cannot leave the group.
	ErrOwnerCannotQuit = New(130005, "owner cannot quit group", http.StatusConflict)
	// ErrMemberMuted indicates the member is muted.
	ErrMemberMuted = New(130006, "member muted", http.StatusForbidden)
	// ADR-0043: group security module (420xx → 1301xx).
	ErrCannotMuteAdmin    = New(130100, "cannot mute owner or admin", http.StatusForbidden)
	ErrMuteAllActive      = New(130101, "group muted", http.StatusForbidden)
	ErrCategoryMuted      = New(130102, "category muted", http.StatusForbidden)
	ErrNewMemberProtected = New(130103, "new member protected", http.StatusForbidden)
	ErrSpamMuted          = New(130104, "rate limited and muted", http.StatusTooManyRequests)
	ErrSensitiveAutoMuted = New(130105, "sensitive word muted", http.StatusForbidden)
)

// Conversation errors (140xxx).
var (
	// ErrConversationNotExist indicates the conversation does not exist.
	ErrConversationNotExist = New(140001, "conversation not exist", http.StatusNotFound)
)

// Message errors (150xxx).
var (
	// ErrMessageEmpty indicates empty message content.
	ErrMessageEmpty = New(150001, "message content empty", http.StatusBadRequest)
	// ErrMessageTooLong indicates oversized message content.
	ErrMessageTooLong = New(150002, "message content too long", http.StatusBadRequest)
	// ErrSensitiveWord indicates the content failed moderation.
	ErrSensitiveWord = New(150003, "sensitive word", http.StatusForbidden)
	// ErrRecallTimeout indicates the recall window has elapsed.
	ErrRecallTimeout = New(150004, "recall timeout", http.StatusConflict)
	// ErrNotMessageSender indicates the caller is not the sender.
	ErrNotMessageSender = New(150005, "not message sender", http.StatusForbidden)
	// ErrDuplicateMessage indicates an idempotent replay. It is a success at
	// the transport level and carries the original server_msg_id and seq.
	ErrDuplicateMessage = New(150006, "duplicate message", http.StatusOK)
	// ErrUploadTypeInvalid indicates an unsupported attachment MIME type.
	ErrUploadTypeInvalid = New(150007, "upload type not supported", http.StatusUnsupportedMediaType)
	// ErrUploadTooLarge indicates an attachment above the configured cap.
	ErrUploadTooLarge = New(150008, "upload too large", http.StatusRequestEntityTooLarge)
	// ErrUploadNotFound indicates a missing stored attachment.
	ErrUploadNotFound = New(150009, "upload object not found", http.StatusNotFound)
	// ErrReactionInvalid indicates an empty or oversized reaction emoji.
	ErrReactionInvalid = New(150010, "reaction invalid", http.StatusBadRequest)
	// ErrReactionTooMany indicates more distinct reactions than one message allows.
	ErrReactionTooMany = New(150011, "too many reactions", http.StatusBadRequest)
	// ADR-0043: burn-after-reading module (410xx → 1501xx).
	ErrBurnTypeNotAllowed = New(150100, "burn not allowed for this message type", http.StatusForbidden)
	ErrBurnTTLInvalid     = New(150101, "burn ttl invalid", http.StatusBadRequest)
	ErrBurnAlreadyBurned  = New(150102, "burn message destroyed", http.StatusGone)
	ErrBurnReadDenied     = New(150103, "not burn message receiver", http.StatusForbidden)
)

// Sync errors (160xxx).
var (
	// ErrSyncVersionTooOld asks the client to run a full sync.
	ErrSyncVersionTooOld = New(160001, "sync version too old", http.StatusConflict)
)

// RTC errors (180xxx).
var (
	// ErrRTCPeerOffline indicates the callee is offline.
	ErrRTCPeerOffline = New(180001, "peer offline", http.StatusNotFound)
	// ErrRTCPeerBusy indicates the callee is busy.
	ErrRTCPeerBusy = New(180002, "peer busy", http.StatusConflict)
	// ErrRTCCallEnded indicates the call has ended.
	ErrRTCCallEnded = New(180003, "call ended", http.StatusConflict)
	// ErrRTCCallNotExist indicates the call does not exist.
	ErrRTCCallNotExist = New(180004, "call not exist", http.StatusNotFound)
	// ErrRTCInviteTimeout indicates the invite timed out.
	ErrRTCInviteTimeout = New(180005, "invite timeout", http.StatusConflict)
	// ErrRTCCallSelf indicates a call to oneself.
	ErrRTCCallSelf = New(180006, "cannot call self", http.StatusBadRequest)
	// ErrRTCBlocked indicates the callee blocked the caller.
	ErrRTCBlocked = New(180007, "blocked by peer", http.StatusForbidden)
	// ErrRTCRoomFull indicates the room is full.
	ErrRTCRoomFull = New(180008, "room full", http.StatusConflict)
	// ErrRTCTokenInvalid indicates an invalid or expired RTC token.
	ErrRTCTokenInvalid = New(180009, "rtc token invalid", http.StatusUnauthorized)
	// ErrRTCNotEnabled indicates RTC is not enabled.
	ErrRTCNotEnabled = New(180010, "rtc not enabled", http.StatusForbidden)
	// ErrRTCGroupBusy indicates another group call is already in progress.
	ErrRTCGroupBusy = New(180011, "group call in progress", http.StatusConflict)
	// ADR-0043: voice room module (440xx → 1801xx).
	ErrVoiceRoomNotExist    = New(180100, "voice room not exist", http.StatusNotFound)
	ErrNoSeatPermission     = New(180101, "no seat permission", http.StatusForbidden)
	ErrSeatOccupied         = New(180102, "seat occupied", http.StatusConflict)
	ErrSeatLocked           = New(180103, "seat locked", http.StatusForbidden)
	ErrRoomMemberRestricted = New(180104, "muted or banned in room", http.StatusForbidden)
	ErrNotInVoiceRoom       = New(180105, "not in voice room", http.StatusForbidden)
	ErrAlreadyHandUp        = New(180106, "already hand up", http.StatusConflict)
	ErrVoiceRoomClosed      = New(180107, "voice room closed", http.StatusGone)
	// ErrVoiceRoomParamInvalid indicates a malformed voice-room request.
	ErrVoiceRoomParamInvalid = New(180108, "voice room param invalid", http.StatusBadRequest)
	// ErrVoiceRoomPasswordWrong indicates a wrong room password.
	ErrVoiceRoomPasswordWrong = New(180109, "voice room password wrong", http.StatusForbidden)
)

// Wallet errors (200xxx, ADR-0043).
var (
	// ErrWalletAccountNotExist indicates the wallet account does not exist.
	ErrWalletAccountNotExist = New(200001, "wallet account not exist", http.StatusNotFound)
	// ErrWalletAccountFrozen indicates the account is frozen.
	ErrWalletAccountFrozen = New(200002, "wallet account frozen", http.StatusForbidden)
	// ErrInsufficientBalance indicates the available balance is not enough.
	ErrInsufficientBalance = New(200003, "insufficient balance", http.StatusPaymentRequired)
	// ErrPayPasswordInvalid indicates a wrong pay password.
	ErrPayPasswordInvalid = New(200004, "pay password invalid", http.StatusForbidden)
	// ErrTOTPInvalid indicates a failed 2FA verification.
	ErrTOTPInvalid = New(200005, "2fa code invalid", http.StatusForbidden)
	// ErrWithdrawFrozen indicates withdrawal inside the 24h cool-down window.
	ErrWithdrawFrozen = New(200010, "withdraw frozen 24h", http.StatusForbidden)
	// ErrWithdrawToInternal indicates withdrawing to an internal deposit address.
	ErrWithdrawToInternal = New(200011, "cannot withdraw to internal deposit address", http.StatusForbidden)
	// ErrWithdrawBelowMin indicates the amount is below the minimum.
	ErrWithdrawBelowMin = New(200012, "withdraw below minimum", http.StatusBadRequest)
	// ErrChainNotSupported indicates an unsupported chain.
	ErrChainNotSupported = New(200013, "chain not supported", http.StatusBadRequest)
	// ErrWithdrawAddressInvalid indicates a malformed withdrawal address.
	ErrWithdrawAddressInvalid = New(200014, "withdraw address invalid", http.StatusBadRequest)
	// ErrRedPacketNotExist indicates the red packet does not exist.
	ErrRedPacketNotExist = New(200201, "red packet not exist", http.StatusNotFound)
	// ErrRedPacketExpired indicates the red packet expired.
	ErrRedPacketExpired = New(200202, "red packet expired", http.StatusGone)
	// ErrRedPacketNotTarget indicates a dedicated red packet claimed by others.
	ErrRedPacketNotTarget = New(200203, "red packet not for you", http.StatusForbidden)
	// ErrRedPacketClaimed indicates the user already claimed.
	ErrRedPacketClaimed = New(200204, "red packet already claimed", http.StatusConflict)
	// ErrRedPacketDepleted indicates the red packet is empty.
	ErrRedPacketDepleted = New(200205, "red packet depleted", http.StatusGone)
	// ErrRedPacketAmountInvalid indicates invalid amount/count for split.
	ErrRedPacketAmountInvalid = New(200206, "red packet amount invalid", http.StatusBadRequest)
	// ErrGiftNotExist indicates the gift does not exist.
	ErrGiftNotExist = New(200301, "gift not exist", http.StatusNotFound)
	// ErrTipSelf indicates tipping oneself.
	ErrTipSelf = New(200302, "cannot tip self", http.StatusForbidden)
	// ErrTipSceneClosed indicates the tip scene is closed.
	ErrTipSceneClosed = New(200303, "tip scene closed", http.StatusGone)
)

// Push errors (190xxx).
var (
	// ErrTargetOffline is a non-error result: it maps to HTTP 200 and tells
	// the caller that no online session received the message.
	ErrTargetOffline = New(190001, "target offline", http.StatusOK)
)

// Admin errors (800xxx).
var (
	// ErrAdminBatchTooLarge indicates an admin batch exceeded 500 items.
	ErrAdminBatchTooLarge = New(800003, "batch exceeds 500 items", http.StatusBadRequest)
)

// Infrastructure errors (900xxx).
var (
	// ErrRateLimited indicates the request was rate limited.
	ErrRateLimited = New(900001, "rate limited", http.StatusTooManyRequests)
	// ErrServerBusy indicates the service is busy.
	ErrServerBusy = New(900002, "server busy", http.StatusServiceUnavailable)
	// ErrInternal indicates an internal failure.
	ErrInternal = New(900003, "internal error", http.StatusInternalServerError)
)

var codeTable = map[int]*Error{
	ErrPasswordIncorrect.Code().Code():      ErrPasswordIncorrect,
	ErrUnauthorized.Code().Code():           ErrUnauthorized,
	ErrTokenKicked.Code().Code():            ErrTokenKicked,
	ErrAccountDisabled.Code().Code():        ErrAccountDisabled,
	ErrMultiDeviceConflict.Code().Code():    ErrMultiDeviceConflict,
	ErrUserNotExist.Code().Code():           ErrUserNotExist,
	ErrNicknameInvalid.Code().Code():        ErrNicknameInvalid,
	ErrUsernameInvalid.Code().Code():        ErrUsernameInvalid,
	ErrPasswordInvalid.Code().Code():        ErrPasswordInvalid,
	ErrUsernameTaken.Code().Code():          ErrUsernameTaken,
	ErrAlreadyFriend.Code().Code():          ErrAlreadyFriend,
	ErrFriendRequestNotExist.Code().Code():  ErrFriendRequestNotExist,
	ErrBlockedByPeer.Code().Code():          ErrBlockedByPeer,
	ErrGroupNotExist.Code().Code():          ErrGroupNotExist,
	ErrNotGroupMember.Code().Code():         ErrNotGroupMember,
	ErrNoGroupPermission.Code().Code():      ErrNoGroupPermission,
	ErrGroupFull.Code().Code():              ErrGroupFull,
	ErrOwnerCannotQuit.Code().Code():        ErrOwnerCannotQuit,
	ErrMemberMuted.Code().Code():            ErrMemberMuted,
	ErrCannotMuteAdmin.Code().Code():        ErrCannotMuteAdmin,
	ErrMuteAllActive.Code().Code():          ErrMuteAllActive,
	ErrCategoryMuted.Code().Code():          ErrCategoryMuted,
	ErrNewMemberProtected.Code().Code():     ErrNewMemberProtected,
	ErrSpamMuted.Code().Code():              ErrSpamMuted,
	ErrSensitiveAutoMuted.Code().Code():     ErrSensitiveAutoMuted,
	ErrConversationNotExist.Code().Code():   ErrConversationNotExist,
	ErrMessageEmpty.Code().Code():           ErrMessageEmpty,
	ErrMessageTooLong.Code().Code():         ErrMessageTooLong,
	ErrSensitiveWord.Code().Code():          ErrSensitiveWord,
	ErrRecallTimeout.Code().Code():          ErrRecallTimeout,
	ErrNotMessageSender.Code().Code():       ErrNotMessageSender,
	ErrDuplicateMessage.Code().Code():       ErrDuplicateMessage,
	ErrUploadTypeInvalid.Code().Code():      ErrUploadTypeInvalid,
	ErrUploadTooLarge.Code().Code():         ErrUploadTooLarge,
	ErrUploadNotFound.Code().Code():         ErrUploadNotFound,
	ErrReactionInvalid.Code().Code():        ErrReactionInvalid,
	ErrReactionTooMany.Code().Code():        ErrReactionTooMany,
	ErrBurnTypeNotAllowed.Code().Code():     ErrBurnTypeNotAllowed,
	ErrBurnTTLInvalid.Code().Code():         ErrBurnTTLInvalid,
	ErrBurnAlreadyBurned.Code().Code():      ErrBurnAlreadyBurned,
	ErrBurnReadDenied.Code().Code():         ErrBurnReadDenied,
	ErrSyncVersionTooOld.Code().Code():      ErrSyncVersionTooOld,
	ErrRTCPeerOffline.Code().Code():         ErrRTCPeerOffline,
	ErrRTCPeerBusy.Code().Code():            ErrRTCPeerBusy,
	ErrRTCCallEnded.Code().Code():           ErrRTCCallEnded,
	ErrRTCCallNotExist.Code().Code():        ErrRTCCallNotExist,
	ErrRTCInviteTimeout.Code().Code():       ErrRTCInviteTimeout,
	ErrRTCCallSelf.Code().Code():            ErrRTCCallSelf,
	ErrRTCBlocked.Code().Code():             ErrRTCBlocked,
	ErrRTCRoomFull.Code().Code():            ErrRTCRoomFull,
	ErrRTCTokenInvalid.Code().Code():        ErrRTCTokenInvalid,
	ErrRTCNotEnabled.Code().Code():          ErrRTCNotEnabled,
	ErrRTCGroupBusy.Code().Code():           ErrRTCGroupBusy,
	ErrVoiceRoomNotExist.Code().Code():      ErrVoiceRoomNotExist,
	ErrNoSeatPermission.Code().Code():       ErrNoSeatPermission,
	ErrSeatOccupied.Code().Code():           ErrSeatOccupied,
	ErrSeatLocked.Code().Code():             ErrSeatLocked,
	ErrVoiceRoomParamInvalid.Code().Code():  ErrVoiceRoomParamInvalid,
	ErrVoiceRoomPasswordWrong.Code().Code(): ErrVoiceRoomPasswordWrong,
	ErrRoomMemberRestricted.Code().Code():   ErrRoomMemberRestricted,
	ErrNotInVoiceRoom.Code().Code():         ErrNotInVoiceRoom,
	ErrAlreadyHandUp.Code().Code():          ErrAlreadyHandUp,
	ErrVoiceRoomClosed.Code().Code():        ErrVoiceRoomClosed,
	ErrTargetOffline.Code().Code():          ErrTargetOffline,
	ErrWalletAccountNotExist.Code().Code():  ErrWalletAccountNotExist,
	ErrWalletAccountFrozen.Code().Code():    ErrWalletAccountFrozen,
	ErrInsufficientBalance.Code().Code():    ErrInsufficientBalance,
	ErrPayPasswordInvalid.Code().Code():     ErrPayPasswordInvalid,
	ErrTOTPInvalid.Code().Code():            ErrTOTPInvalid,
	ErrWithdrawFrozen.Code().Code():         ErrWithdrawFrozen,
	ErrWithdrawToInternal.Code().Code():     ErrWithdrawToInternal,
	ErrWithdrawBelowMin.Code().Code():       ErrWithdrawBelowMin,
	ErrChainNotSupported.Code().Code():      ErrChainNotSupported,
	ErrWithdrawAddressInvalid.Code().Code(): ErrWithdrawAddressInvalid,
	ErrRedPacketNotExist.Code().Code():      ErrRedPacketNotExist,
	ErrRedPacketExpired.Code().Code():       ErrRedPacketExpired,
	ErrRedPacketNotTarget.Code().Code():     ErrRedPacketNotTarget,
	ErrRedPacketClaimed.Code().Code():       ErrRedPacketClaimed,
	ErrRedPacketDepleted.Code().Code():      ErrRedPacketDepleted,
	ErrRedPacketAmountInvalid.Code().Code(): ErrRedPacketAmountInvalid,
	ErrGiftNotExist.Code().Code():           ErrGiftNotExist,
	ErrTipSelf.Code().Code():                ErrTipSelf,
	ErrTipSceneClosed.Code().Code():         ErrTipSceneClosed,
	ErrAdminBatchTooLarge.Code().Code():     ErrAdminBatchTooLarge,
	ErrRateLimited.Code().Code():            ErrRateLimited,
	ErrServerBusy.Code().Code():             ErrServerBusy,
	ErrInternal.Code().Code():               ErrInternal,
}

func byCode(code int) (*Error, bool) {
	if code == CodeOK {
		return nil, false
	}
	item, ok := codeTable[code]
	return item, ok
}

func statusBySegment(code int) int {
	switch {
	case code >= 100000 && code < 110000:
		return http.StatusUnauthorized
	case code >= 110000 && code < 120000:
		return http.StatusNotFound
	case code >= 120000 && code < 130000:
		return http.StatusConflict
	case code >= 130000 && code < 140000:
		return http.StatusForbidden
	case code >= 140000 && code < 150000:
		return http.StatusNotFound
	case code >= 150000 && code < 160000:
		return http.StatusBadRequest
	case code >= 160000 && code < 170000:
		return http.StatusConflict
	case code >= 170000 && code < 180000:
		return http.StatusBadRequest
	case code >= 180000 && code < 190000:
		return http.StatusConflict
	case code >= 190000 && code < 200000:
		return http.StatusOK
	case code >= 200000 && code < 210000:
		// Wallet segment (ADR-0043). Declared codes carry their own status;
		// this is only the fallback for unknown wallet codes.
		return http.StatusForbidden
	case code >= 800000 && code < 900000:
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

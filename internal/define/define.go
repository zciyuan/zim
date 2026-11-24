// Package define holds application-wide constants shared by the ZIM
// processes. Environment-specific values belong in etc, not here.
package define

// Process and binary names. The 13 frozen names come from ADR-0002 and
// ADR-0043 (which added voiceroom and wallet).
const (
	ServiceGate         = "zim-gate"
	ServiceNodeMsg      = "zim-node-msg"
	ServiceNodeConv     = "zim-node-conv"
	ServiceMeshAuth     = "zim-mesh-auth"
	ServiceMeshUser     = "zim-mesh-user"
	ServiceMeshRelation = "zim-mesh-relation"
	ServiceMeshGroup    = "zim-mesh-group"
	ServiceVoiceroom    = "zim-voiceroom"
	ServiceTransfer     = "zim-transfer"
	ServicePush         = "zim-push"
	ServiceAPI          = "zim-api"
	ServiceCrontask     = "zim-crontask"
	ServiceWallet       = "zim-wallet" // ADR-0043: 独立 wallet mesh 二进制
)

// Mesh service names. The discovery target is discovery://<name>.
const (
	ServiceNameAuth      = "auth"
	ServiceNameUser      = "user"
	ServiceNameRelation  = "relation"
	ServiceNameGroup     = "group"
	ServiceNameVoiceroom = "voiceroom"
	ServiceNameWallet    = "wallet"

	AuthTarget      = "discovery://" + ServiceNameAuth
	UserTarget      = "discovery://" + ServiceNameUser
	RelationTarget  = "discovery://" + ServiceNameRelation
	GroupTarget     = "discovery://" + ServiceNameGroup
	VoiceroomTarget = "discovery://" + ServiceNameVoiceroom
	WalletTarget    = "discovery://" + ServiceNameWallet
)

// Protocol and transport constants.
const (
	Protocol       = "zim/1"
	ProtocolHeader = "X-Protocol"
)

// Package define holds application-wide constants shared by the ZIM
// processes. Environment-specific values belong in etc, not here.
package define

// Process and binary names. They are the 11 frozen names from ADR-0002.
const (
	ServiceGate         = "zim-gate"
	ServiceNodeMsg      = "zim-node-msg"
	ServiceNodeConv     = "zim-node-conv"
	ServiceMeshAuth     = "zim-mesh-auth"
	ServiceMeshUser     = "zim-mesh-user"
	ServiceMeshRelation = "zim-mesh-relation"
	ServiceMeshGroup    = "zim-mesh-group"
	ServiceTransfer     = "zim-transfer"
	ServicePush         = "zim-push"
	ServiceAPI          = "zim-api"
	ServiceCrontask     = "zim-crontask"
)

// Mesh service names. The discovery target is discovery://<name>.
const (
	ServiceNameAuth     = "auth"
	ServiceNameUser     = "user"
	ServiceNameRelation = "relation"
	ServiceNameGroup    = "group"

	AuthTarget     = "discovery://" + ServiceNameAuth
	UserTarget     = "discovery://" + ServiceNameUser
	RelationTarget = "discovery://" + ServiceNameRelation
	GroupTarget    = "discovery://" + ServiceNameGroup
)

// Protocol and transport constants.
const (
	Protocol       = "zim/1"
	ProtocolHeader = "X-Protocol"
)

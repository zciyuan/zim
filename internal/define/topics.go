package define

// Kafka pipeline topics (frozen names, ZIM-01 §22).
const (
	TopicToRedis       = "zim.pipe.toRedis"
	TopicToMongo       = "zim.pipe.toMongo"
	TopicToPush        = "zim.pipe.toPush"
	TopicToOfflinePush = "zim.pipe.toOfflinePush"

	// ADR-0043: burn/DLP/group-status event topics.
	TopicBurnMediaDelete    = "zim.pipe.burnMediaDelete"
	TopicGroupMemberStatus  = "zim.pipe.groupMemberStatus"
	TopicDLPScreenshotAudit = "zim.pipe.dlpScreenshotAudit"
	TopicWalletRpClaim      = "zim.pipe.walletRpClaim"
	TopicWalletRpRefund     = "zim.pipe.walletRpRefund"
	TopicWalletTipSettled   = "zim.pipe.walletTipSettled"

	ConsumerGroupTransfer        = "zim-transfer"
	ConsumerGroupPush            = "zim-push"
	ConsumerGroupOffline         = "zim-push-offline"
	ConsumerGroupVoiceroomStatus = "zim-voiceroom-status"
	ConsumerGroupBurnMedia       = "zim-crontask-burn-media"

	DLQSuffix = ".DLQ"
)

// TopicDLQ returns the dead-letter topic for a pipeline topic.
func TopicDLQ(topic string) string {
	return topic + DLQSuffix
}

package define

// Kafka pipeline topics (frozen names, ZIM-01 §22).
const (
	TopicToRedis       = "zim.pipe.toRedis"
	TopicToMongo       = "zim.pipe.toMongo"
	TopicToPush        = "zim.pipe.toPush"
	TopicToOfflinePush = "zim.pipe.toOfflinePush"

	ConsumerGroupTransfer = "zim-transfer"
	ConsumerGroupPush     = "zim-push"
	ConsumerGroupOffline  = "zim-push-offline"

	DLQSuffix = ".DLQ"
)

// TopicDLQ returns the dead-letter topic for a pipeline topic.
func TopicDLQ(topic string) string {
	return topic + DLQSuffix
}

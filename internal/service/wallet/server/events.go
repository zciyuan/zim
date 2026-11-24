package wallet

import (
	"context"
	"strconv"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// EventPublisher publishes wallet side effects to the ADR-0043 Kafka
// topics (zim.pipe.walletRpClaim / walletRpRefund / walletTipSettled). It is
// at-least-once: consumers dedupe by their own idempotency keys.
type EventPublisher interface {
	PublishRpClaim(ctx context.Context, rpID, claimerUID, amount int64) error
	PublishRpRefund(ctx context.Context, rpID, senderUID, refund int64) error
	PublishTipSettled(ctx context.Context, tip *walletdao.TipOrder) error
}

// kafkaWalletEvents is the Kafka-backed publisher.
type kafkaWalletEvents struct {
	producer pipe.Producer
}

// NewKafkaEvents returns a Kafka-backed wallet event publisher.
func NewKafkaEvents(producer pipe.Producer) EventPublisher {
	return &kafkaWalletEvents{producer: producer}
}

func (p *kafkaWalletEvents) publish(ctx context.Context, topic, key string, value any) error {
	if p == nil || p.producer == nil {
		return nil
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return p.producer.Publish(ctx, pipe.Message{
		Topic: topic, Key: key, Value: body,
		Headers: map[string]string{"content-type": "application/json"},
	})
}

func (p *kafkaWalletEvents) PublishRpClaim(ctx context.Context, rpID, claimerUID, amount int64) error {
	return p.publish(ctx, define.TopicWalletRpClaim, strconv.FormatInt(rpID, 10), map[string]any{
		"rp_id": rpID, "claimer_uid": claimerUID, "amount": amount,
	})
}

func (p *kafkaWalletEvents) PublishRpRefund(ctx context.Context, rpID, senderUID, refund int64) error {
	return p.publish(ctx, define.TopicWalletRpRefund, strconv.FormatInt(rpID, 10), map[string]any{
		"rp_id": rpID, "sender_uid": senderUID, "refund": refund,
	})
}

func (p *kafkaWalletEvents) PublishTipSettled(ctx context.Context, tip *walletdao.TipOrder) error {
	return p.publish(ctx, define.TopicWalletTipSettled, strconv.FormatInt(tip.ReceiverUID, 10), map[string]any{
		"tip_id": tip.TipID, "biz_order_id": tip.BizOrderID, "scene": tip.Scene,
		"scene_id": tip.SceneID, "sender_uid": tip.SenderUID, "receiver_uid": tip.ReceiverUID,
		"gift_id": tip.GiftID, "gift_count": tip.GiftCount, "combo_id": tip.ComboID,
		"gross_amount": tip.GrossAmount, "net_amount": tip.NetAmount,
	})
}

// publishWalletEvent best-effort publishes one event; failures are logged and
// never fail the money transaction (the ledger is already committed).
func (s *Service) publishWalletEvent(publish func() error, desc string) {
	if s == nil || s.events == nil {
		return
	}
	if err := publish(); err != nil {
		log.Warnf("wallet event publish failed: %s, err = %v", desc, err)
	}
}

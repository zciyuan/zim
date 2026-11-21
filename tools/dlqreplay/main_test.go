package main

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestResolveTopic(t *testing.T) {
	if got := resolveTopic("zim.pipe.toRedis", nil); got != "zim.pipe.toRedis" {
		t.Fatalf("override = %q", got)
	}
	headers := []kafka.Header{
		{Key: "x-original-topic", Value: []byte("zim.pipe.toPush")},
		{Key: "other", Value: []byte("x")},
	}
	if got := resolveTopic("", headers); got != "zim.pipe.toPush" {
		t.Fatalf("header = %q", got)
	}
	if got := resolveTopic("", nil); got != "" {
		t.Fatalf("empty = %q", got)
	}
}

func TestSplitBrokers(t *testing.T) {
	brokers := splitBrokers(" 127.0.0.1:9092 , kafka:9092 ,,")
	if len(brokers) != 2 || brokers[0] != "127.0.0.1:9092" || brokers[1] != "kafka:9092" {
		t.Fatalf("brokers = %#v", brokers)
	}
}

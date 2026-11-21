// Command dlqreplay replays records from a <topic>.DLQ back to their original
// topic. It is an operator tool, not one of the 11 ZIM binaries.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", "127.0.0.1:9092", "Kafka bootstrap servers, comma separated")
	dlq := flag.String("dlq", "", "dead-letter topic, e.g. zim.pipe.toRedis.DLQ")
	target := flag.String("target", "", "override the original topic; default reads x-original-topic header")
	limit := flag.Int("limit", 100, "maximum records to replay")
	dryRun := flag.Bool("dry-run", false, "print records without publishing")
	idle := flag.Duration("idle-timeout", 3*time.Second, "stop a partition after this idle period")
	flag.Parse()

	if *dlq == "" {
		fmt.Fprintln(os.Stderr, "-dlq is required")
		os.Exit(2)
	}
	addresses := splitBrokers(*brokers)
	if len(addresses) == 0 {
		fmt.Fprintln(os.Stderr, "-brokers is required")
		os.Exit(2)
	}
	if err := replay(context.Background(), addresses, *dlq, *target, *limit, *dryRun, *idle); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func replay(ctx context.Context, brokers []string, dlq, target string, limit int, dryRun bool, idle time.Duration) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer func() { _ = conn.Close() }()
	partitions, err := conn.ReadPartitions(dlq)
	if err != nil {
		return fmt.Errorf("read partitions of %s: %w", dlq, err)
	}

	writer := &kafka.Writer{
		Addr: kafka.TCP(brokers...), Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, BatchTimeout: 5 * time.Millisecond,
	}
	defer func() { _ = writer.Close() }()

	total := 0
	for _, partition := range partitions {
		if total >= limit {
			break
		}
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, Topic: dlq, Partition: partition.ID,
			StartOffset: kafka.FirstOffset, MaxWait: time.Second,
		})
		for total < limit {
			readCtx, cancel := context.WithTimeout(ctx, idle)
			message, readErr := reader.ReadMessage(readCtx)
			cancel()
			if readErr != nil {
				break
			}
			topic := resolveTopic(target, message.Headers)
			if topic == "" {
				_ = reader.Close()
				return fmt.Errorf("record at offset %d has no x-original-topic", message.Offset)
			}
			if dryRun {
				fmt.Printf("dry-run: partition=%d offset=%d -> %s key=%s bytes=%d\n",
					partition.ID, message.Offset, topic, string(message.Key), len(message.Value))
				total++
				continue
			}
			if err = writer.WriteMessages(ctx, kafka.Message{
				Topic: topic, Key: message.Key, Value: message.Value, Headers: message.Headers,
			}); err != nil {
				_ = reader.Close()
				return fmt.Errorf("replay partition %d offset %d: %w", partition.ID, message.Offset, err)
			}
			total++
		}
		_ = reader.Close()
	}
	fmt.Printf("replayed %d records from %s\n", total, dlq)
	return nil
}

func resolveTopic(target string, headers []kafka.Header) string {
	if target != "" {
		return target
	}
	for _, header := range headers {
		if header.Key == "x-original-topic" {
			return string(header.Value)
		}
	}
	return ""
}

func splitBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			brokers = append(brokers, part)
		}
	}
	return brokers
}

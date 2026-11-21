// Command seqrepair raises per-conversation sequence floors from MongoDB.
//
// A Redis loss can reset zim:seq:{conv} to zero. This tool scans the max seq
// already persisted for the requested conversations and raises the floor with
// a compare-and-set Lua script, never lowering a concurrent allocator.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	mongoURI := flag.String("mongo", "mongodb://127.0.0.1:27017", "MongoDB URI")
	dbName := flag.String("db", "zim", "MongoDB database")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	convs := flag.String("convs", "", "comma-separated conversation IDs")
	flag.Parse()
	if strings.TrimSpace(*convs) == "" {
		log.Fatal("convs is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*mongoURI))
	if err != nil {
		log.Println(err)
		return
	}
	defer func() { _ = client.Disconnect(ctx) }()
	cache := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer func() { _ = cache.Close() }()

	for _, conv := range strings.Split(*convs, ",") {
		conv = strings.TrimSpace(conv)
		if conv == "" {
			continue
		}
		maxSeq, err := maxSeq(ctx, client.Database(*dbName).Collection("msg"), conv)
		if err != nil {
			log.Printf("scan %s: %v", conv, err)
		}
		raised, err := raiseFloor(ctx, cache, conv, maxSeq)
		if err != nil {
			log.Printf("repair %s: %v", conv, err)
		}
		_, _ = fmt.Fprintf(os.Stdout, "conv=%s max_seq=%d raised=%v\n", conv, maxSeq, raised)
	}
}

func maxSeq(ctx context.Context, blocks *mongo.Collection, conv string) (int64, error) {
	cursor, err := blocks.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "conversation_id", Value: conv}}}},
		bson.D{{Key: "$unwind", Value: "$msgs"}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "max", Value: bson.D{{Key: "$max", Value: "$msgs.msg.seq"}}},
		}}},
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = cursor.Close(ctx) }()
	var result struct {
		Max int64 `bson:"max"`
	}
	if cursor.Next(ctx) {
		if err = cursor.Decode(&result); err != nil {
			return 0, err
		}
	}
	return result.Max, cursor.Err()
}

var raiseScript = redis.NewScript(`
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local floor = tonumber(ARGV[1])
if current < floor then
  redis.call('SET', KEYS[1], floor)
  return 1
end
return 0
`)

func raiseFloor(ctx context.Context, cache *redis.Client, conv string, floor int64) (bool, error) {
	if floor <= 0 {
		return false, nil
	}
	result, err := raiseScript.Run(ctx, cache, []string{"zim:seq:" + conv}, floor).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

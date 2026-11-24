package message

import (
	"context"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"

	"github.com/zciyuan/ZIM/internal/model"
)

func TestAppendRepairsPendingIdem(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("repair pending", func(mt *mtest.T) {
		dao := New(mt.DB)
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 11000, Message: "duplicate key"}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+"."+idemCollection, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: "s_1_2|c1"},
				{Key: "server_msg_id", Value: "srv-1"},
				{Key: "seq", Value: int64(42)},
				{Key: "state", Value: idemPending},
			}),
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nModified", Value: 1}}...),
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nModified", Value: 1}}...),
		)

		result, err := dao.appendDirect(context.Background(), &model.Message{
			ServerMsgID: "srv-new", ClientMsgID: "c1", ConversationID: "s_1_2", Seq: 99,
			SendID: 1, RecvID: 2, MsgType: 1, Content: "hello",
		})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Duplicate || result.ServerMsgID != "srv-1" || result.Seq != 42 {
			t.Fatalf("result = %+v", result)
		}
	})
}

func TestAppendCommittedReturnsDuplicate(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("committed", func(mt *mtest.T) {
		dao := New(mt.DB)
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 11000, Message: "duplicate key"}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+"."+idemCollection, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: "s_1_2|c1"},
				{Key: "server_msg_id", Value: "srv-1"},
				{Key: "seq", Value: int64(42)},
				{Key: "state", Value: idemCommitted},
			}),
		)

		result, err := dao.appendDirect(context.Background(), &model.Message{
			ServerMsgID: "srv-new", ClientMsgID: "c1", ConversationID: "s_1_2", Seq: 99,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Duplicate || result.ServerMsgID != "srv-1" || result.Seq != 42 {
			t.Fatalf("result = %+v", result)
		}
	})
}

func TestAppendWritesBlockThenCommits(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("new message", func(mt *mtest.T) {
		dao := New(mt.DB)
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(),
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nModified", Value: 1}}...),
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nModified", Value: 1}}...),
		)

		result, err := dao.appendDirect(context.Background(), &model.Message{
			ServerMsgID: "srv-1", ClientMsgID: "c1", ConversationID: "s_1_2", Seq: 42,
			SendID: 1, RecvID: 2, MsgType: 1, Content: "hello",
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Duplicate || result.ServerMsgID != "srv-1" || result.Seq != 42 {
			t.Fatalf("result = %+v", result)
		}
	})
}

func TestAppendUsesBatchedMongoCommands(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("batch", func(mt *mtest.T) {
		dao := New(mt.DB)
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nInserted", Value: 1}}...),
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nModified", Value: 1}}...),
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nModified", Value: 1}}...),
		)
		result, err := dao.Append(context.Background(), &model.Message{
			ServerMsgID: "srv-batch", ClientMsgID: "cb", ConversationID: "s_1_2",
			Seq: 1, SendID: 1, RecvID: 2, MsgType: 1, Content: "hello",
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.ServerMsgID != "srv-batch" || result.Seq != 1 {
			t.Fatalf("result = %+v", result)
		}
	})
}

// TestAppendBatchBurnFields 批量路径必须与 appendDirect 一致地落盘
// burn 字段并把负载写入 burn_messages（ADR-0043 §2.4.1）。曾因 flush 遗漏
// 字段映射导致生产环境阅后即焚静默失效。
func TestAppendBatchBurnFields(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("burn via batch", func(mt *mtest.T) {
		dao := New(mt.DB)
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nInserted", Value: 1}}...),
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nModified", Value: 1}}...),
			mtest.CreateSuccessResponse(),
			mtest.CreateSuccessResponse(bson.D{{Key: "n", Value: 1}, {Key: "nModified", Value: 1}}...),
		)
		_, err := dao.Append(context.Background(), &model.Message{
			ServerMsgID: "srv-burn", ClientMsgID: "cburn", ConversationID: "s_1_2",
			Seq: 1, SendID: 1, RecvID: 2, MsgType: 1, Content: "secret",
			BurnTTL: 10, BurnExpireAt: 123456, ExtType: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		var (
			burnStore   bool
			blockUpdate bool
		)
		for _, event := range mt.GetAllStartedEvents() {
			if event.Command == nil {
				continue
			}
			switch event.CommandName {
			case "insert":
				if target, ok := event.Command.Lookup("insert").StringValueOK(); ok && target == burnCollection {
					burnStore = true
				}
			case "update":
				// StoreBurnPayload is ReplaceOne (upsert) against burn_messages;
				// the block $push must carry burn_ttl with empty content.
				if target, ok := event.Command.Lookup("update").StringValueOK(); ok && target == burnCollection {
					burnStore = true
					continue
				}
				raw := event.Command.String()
				if strings.Contains(raw, `"burn_ttl": {"$numberInt":"10"}`) && strings.Contains(raw, `"content": ""`) {
					blockUpdate = true
				}
			}
		}
		if !burnStore {
			for _, event := range mt.GetAllStartedEvents() {
				t.Logf("started: %s", event.CommandName)
			}
			t.Fatal("burn payload must be stored in burn_messages")
		}
		if !blockUpdate {
			for _, event := range mt.GetAllStartedEvents() {
				if event.CommandName != "update" {
					continue
				}
				if target, ok := event.Command.Lookup("update").StringValueOK(); ok && target == burnCollection {
					continue
				}
				t.Logf("block update: %s", event.Command)
			}
			t.Fatal("block must carry burn_ttl and empty content")
		}
	})
}

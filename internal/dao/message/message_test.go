package message

import (
	"context"
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

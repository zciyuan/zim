package message

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"

	"github.com/zciyuan/ZIM/internal/model"
)

func TestAppendSeqConflict(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("seq conflict", func(mt *mtest.T) {
		dao := New(mt.DB)
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 11000, Message: "duplicate key"}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+"."+blocksCollection, mtest.FirstBatch,
				bson.D{{Key: "n", Value: int32(0)}}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+"."+blocksCollection, mtest.FirstBatch,
				bson.D{{Key: "n", Value: int32(1)}}),
		)
		err := dao.writeBlock(context.Background(), &model.Message{
			ServerMsgID: "srv-new", ClientMsgID: "c1", ConversationID: "s_1_2", Seq: 99,
			SendID: 1, RecvID: 2, MsgType: 1, Content: "hello",
		})
		if err == nil {
			t.Fatal("expected seq conflict")
		}
	})
}

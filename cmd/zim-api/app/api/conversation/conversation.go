// Package conversation exposes conversation REST endpoints.
package conversation

import (
	"strconv"

	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	historysvc "github.com/zciyuan/ZIM/internal/service/history/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// NewAPI returns the conversation REST module.
func NewAPI(service *conversationsvc.Service, history *historysvc.Service) *API {
	return &API{service: service, history: history}
}

// List returns the caller's conversations.
func (a *API) List(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	items, err := a.service.List(ctx.Context(), middleware.UID(ctx), 0, 100)
	if err != nil {
		return api.Failure(ctx, err)
	}
	return api.Success(ctx, &listRes{Items: items})
}

// Read advances the read position.
func (a *API) Read(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	req := &readReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrConversationNotExist)
	}
	seq, err := a.service.Read(ctx.Context(), middleware.UID(ctx), ctx.Params("cid"), req.Seq)
	if err != nil {
		return api.Failure(ctx, err)
	}
	return api.Success(ctx, &readRes{ReadSeq: seq})
}

// Unread returns the unread count.
func (a *API) Unread(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	unread, err := a.service.Unread(ctx.Context(), middleware.UID(ctx), ctx.Params("cid"))
	if err != nil {
		return api.Failure(ctx, err)
	}
	return api.Success(ctx, &unreadRes{Unread: unread})
}

// Pin updates the pin flag.
func (a *API) Pin(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	req := &pinReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrConversationNotExist)
	}
	if err := a.service.Pin(ctx.Context(), middleware.UID(ctx), ctx.Params("cid"), req.Pinned); err != nil {
		return api.Failure(ctx, err)
	}
	return api.Success(ctx, &operationRes{OK: true})
}

// Delete marks a conversation deleted.
func (a *API) Delete(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	if err := a.service.Delete(ctx.Context(), middleware.UID(ctx), ctx.Params("cid")); err != nil {
		return api.Failure(ctx, err)
	}
	return api.Success(ctx, &operationRes{OK: true})
}

// Messages returns a seq range of message history.
func (a *API) Messages(ctx http.Context) error {
	if a.history == nil || a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	uid := middleware.UID(ctx)
	min, max, err := a.service.VisibleRange(ctx.Context(), uid, ctx.Params("cid"))
	if err != nil {
		return api.Failure(ctx, err)
	}
	begin, _ := strconv.ParseInt(ctx.Query("begin_seq"), 10, 64)
	end, _ := strconv.ParseInt(ctx.Query("end_seq"), 10, 64)
	if begin == 0 {
		begin, _ = strconv.ParseInt(ctx.Query("begin"), 10, 64)
	}
	if end == 0 {
		end, _ = strconv.ParseInt(ctx.Query("end"), 10, 64)
	}
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	if begin < min {
		begin = min
	}
	if end <= 0 || end > max {
		end = max
	}
	if begin > end {
		return api.Success(ctx, &messagesRes{Messages: []*messageRes{}, HasMore: false})
	}
	messages, hasMore, err := a.history.Pull(ctx.Context(), ctx.Params("cid"), begin, end, limit)
	if err != nil {
		return api.Failure(ctx, err)
	}
	items := make([]*messageRes, 0, len(messages))
	for _, item := range messages {
		items = append(items, &messageRes{
			ServerMsgID: item.ServerMsgID, ClientMsgID: item.ClientMsgID, Seq: item.Seq,
			SendID: item.SendID, RecvID: item.RecvID, GroupID: item.GroupID,
			MsgType: item.MsgType, Content: item.Content, AtUserIDs: item.AtUserIDs,
			SendTime: item.SendTime, Status: item.Status,
		})
	}
	return api.Success(ctx, &messagesRes{Messages: items, HasMore: hasMore})
}

// Clear hides all messages up to the current latest seq.
func (a *API) Clear(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	seq, err := a.service.Clear(ctx.Context(), middleware.UID(ctx), ctx.Params("cid"))
	if err != nil {
		return api.Failure(ctx, err)
	}
	return api.Success(ctx, &clearRes{MinSeq: seq})
}

// Active returns the most recent conversations.
func (a *API) Active(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	items, err := a.service.List(ctx.Context(), middleware.UID(ctx), 0, limit)
	if err != nil {
		return api.Failure(ctx, err)
	}
	return api.Success(ctx, &listRes{Items: items})
}

// Search finds messages containing a keyword in one conversation.
func (a *API) Search(ctx http.Context) error {
	if a.history == nil || a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	minSeq, _, err := a.service.VisibleRange(ctx.Context(), middleware.UID(ctx), ctx.Params("cid"))
	if err != nil {
		return api.Failure(ctx, err)
	}
	keyword := ctx.Query("q")
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	items, err := a.history.Search(ctx.Context(), ctx.Params("cid"), keyword, limit)
	if err != nil {
		return api.Failure(ctx, err)
	}
	visible := make([]*searchItem, 0, len(items))
	for _, item := range items {
		if item.Seq >= minSeq {
			visible = append(visible, &searchItem{
				ServerMsgID: item.ServerMsgID, ConversationID: item.ConversationID,
				Seq: item.Seq, SendID: item.SendID, RecvID: item.RecvID, GroupID: item.GroupID,
				MsgType: item.MsgType, Content: item.Content, SendTime: item.SendTime, Status: item.Status,
			})
		}
	}
	return api.Success(ctx, map[string]any{"items": visible})
}

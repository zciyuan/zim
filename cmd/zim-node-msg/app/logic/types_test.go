package logic

import "github.com/zciyuan/ZIM/zimfw/contract"

type fakeNodeContext struct {
	envelope    *contract.Envelope
	parseErr    error
	response    *contract.Envelope
	responseErr error
}

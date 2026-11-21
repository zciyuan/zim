package route

type fakeLocator struct {
	locateGID string
	locateKey int64
	bindGID   string
	bindCID   int64
	bindKey   int64
	unbindKey int64
	err       error
}

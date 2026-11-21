package webtransport

import (
	"io"
	"net"
	"time"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/core/queue"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"github.com/dobyte/due/v2/network"
	"github.com/dobyte/due/v2/packet"
	taskpool "github.com/dobyte/due/v2/task"
	"github.com/dobyte/due/v2/utils/xnet"
	wt "github.com/quic-go/webtransport-go"
)

const minWriteQueueSize = 128

var _ network.Conn = (*serverConn)(nil)

// newServerConn creates one due connection for a WebTransport session.
func newServerConn(mgr *connMgr, session *wt.Session, stream *wt.Stream) *serverConn {
	conn := &serverConn{
		attr:    &attr{},
		mgr:     mgr,
		session: session,
		stream:  stream,
		queue: queue.NewQueue[buffer.Buffer](
			int32(max(minWriteQueueSize, mgr.server.opts.writeQueueSize)),
			mgr.server.opts.writeTimeout,
		),
	}
	conn.state.Store(int32(network.ConnOpened))
	conn.lastHeartbeatTime.Store(time.Now().UnixNano())
	return conn
}

// start launches the read and write goroutines. It is a no-op after the
// connection has already been closed; the lock serializes the wait-group
// registration with doClose so that a concurrent Stop cannot Wait before Add.
func (c *serverConn) start() {
	c.rw.Lock()
	if c.state.Load() != int32(network.ConnOpened) {
		c.rw.Unlock()
		return
	}
	c.wgRead.Add(1)
	c.wgWrite.Add(1)
	c.started = true
	c.rw.Unlock()

	go c.runRead()
	go c.runWrite()
}

// runRead runs the connect hook and then the read loop.
func (c *serverConn) runRead() {
	defer c.wgRead.Done()

	if c.State() == network.ConnOpened {
		c.checkAuthorize()
		if c.mgr.server.connectHandler != nil {
			c.mgr.server.connectHandler(c)
		}
	}

	c.read()
}

// ID returns the connection ID.
func (c *serverConn) ID() int64 {
	return c.id
}

// UID returns the bound user ID, or zero when unbound.
func (c *serverConn) UID() int64 {
	return c.uid.Load()
}

// Attr returns the connection attributes.
func (c *serverConn) Attr() network.Attr {
	return c.attr
}

// Bind binds a user ID to the connection.
func (c *serverConn) Bind(uid int64) error {
	c.rw.RLock()
	if c.isClosed() {
		c.rw.RUnlock()
		return errors.ErrConnectionClosed
	}
	c.uid.Store(uid)
	c.uncheckAuthorize()
	c.rw.RUnlock()
	return nil
}

// Unbind clears the bound user ID and restarts the authorization timer.
func (c *serverConn) Unbind() error {
	c.rw.RLock()
	if c.isClosed() {
		c.rw.RUnlock()
		return errors.ErrConnectionClosed
	}
	c.uid.Store(0)
	c.rw.RUnlock()

	c.checkAuthorize()
	return nil
}

// Push queues one frame for the session stream.
func (c *serverConn) Push(buf buffer.Buffer) error {
	if buf == nil || buf.Len() == 0 {
		return errors.ErrInvalidMessage
	}

	c.rw.RLock()
	if err := c.checkState(); err != nil {
		c.rw.RUnlock()
		return err
	}
	err := c.queue.Write(buf)
	c.rw.RUnlock()
	return err
}

// State returns the connection state.
func (c *serverConn) State() network.ConnState {
	return network.ConnState(c.state.Load())
}

// Close closes the connection. When force is true it does not drain the write
// queue.
func (c *serverConn) Close(force ...bool) error {
	if len(force) > 0 && force[0] {
		return c.forceClose()
	}
	return c.graceClose()
}

// CloseIf forcibly closes the connection when it still carries the given ID.
func (c *serverConn) CloseIf(id int64) error {
	c.rw.Lock()
	if c.id != id || c.session == nil || c.state.Swap(int32(network.ConnClosed)) == int32(network.ConnClosed) {
		c.rw.Unlock()
		return errors.ErrConnectionClosed
	}
	c.rw.Unlock()

	c.uncheckAuthorize()
	return c.doClose(false)
}

// LocalIP returns the local IP address.
func (c *serverConn) LocalIP() (string, error) {
	addr, err := c.LocalAddr()
	if err != nil {
		return "", err
	}
	return xnet.ExtractIP(addr)
}

// LocalAddr returns the local UDP address.
func (c *serverConn) LocalAddr() (net.Addr, error) {
	c.rw.RLock()
	defer c.rw.RUnlock()

	if c.session == nil {
		return nil, errors.ErrConnectionClosed
	}
	return c.session.LocalAddr(), nil
}

// RemoteIP returns the remote IP address.
func (c *serverConn) RemoteIP() (string, error) {
	addr, err := c.RemoteAddr()
	if err != nil {
		return "", err
	}
	return xnet.ExtractIP(addr)
}

// RemoteAddr returns the remote UDP address.
func (c *serverConn) RemoteAddr() (net.Addr, error) {
	c.rw.RLock()
	defer c.rw.RUnlock()

	if c.session == nil {
		return nil, errors.ErrConnectionClosed
	}
	return c.session.RemoteAddr(), nil
}

// checkState maps the connection state to the matching due error.
func (c *serverConn) checkState() error {
	switch c.State() {
	case network.ConnHanged:
		return errors.ErrConnectionHanged
	case network.ConnClosed:
		return errors.ErrConnectionClosed
	default:
		return nil
	}
}

// isClosed reports whether the connection is closed.
func (c *serverConn) isClosed() bool {
	return c.State() == network.ConnClosed
}

// checkAuthorize starts the authorization timeout when one is configured.
func (c *serverConn) checkAuthorize() {
	timeout := c.mgr.server.opts.authorizeTimeout
	if timeout <= 0 {
		return
	}

	c.rw.RLock()
	stream := c.stream
	c.rw.RUnlock()
	if stream == nil {
		return
	}

	id := c.id
	timer := time.AfterFunc(timeout, func() {
		if c.UID() == 0 {
			c.recycleClose(stream, id)
		}
	})
	if old := c.authorizeTimer.Swap(timer); old != nil {
		old.Stop()
	}
}

// uncheckAuthorize stops the authorization timer.
func (c *serverConn) uncheckAuthorize() {
	if timer := c.authorizeTimer.Swap(nil); timer != nil {
		timer.Stop()
	}
}

// graceClose drains the write queue before closing the session.
func (c *serverConn) graceClose() error {
	if !c.state.CompareAndSwap(int32(network.ConnOpened), int32(network.ConnHanged)) {
		return errors.ErrConnectionNotOpened
	}
	c.uncheckAuthorize()

	c.rw.RLock()
	if c.session == nil {
		c.rw.RUnlock()
		return errors.ErrConnectionClosed
	}
	session := c.session
	q := c.queue
	err := q.Write(buffer.NewBytes(nil))
	c.rw.RUnlock()

	if err == nil {
		if timeout := c.mgr.server.opts.closeTimeout; timeout > 0 {
			timer := time.AfterFunc(timeout, func() {
				_ = session.CloseWithError(0, "close timeout")
			})
			q.Wait()
			timer.Stop()
		} else {
			q.Wait()
		}
	}

	if c.state.Swap(int32(network.ConnClosed)) == int32(network.ConnClosed) {
		return errors.ErrConnectionClosed
	}
	return c.doClose(true)
}

// forceClose closes the session without draining the write queue.
func (c *serverConn) forceClose() error {
	if c.state.Swap(int32(network.ConnClosed)) == int32(network.ConnClosed) {
		return errors.ErrConnectionClosed
	}
	c.uncheckAuthorize()
	return c.doClose(false)
}

// recycleClose closes the connection only if it is still the observed one.
func (c *serverConn) recycleClose(stream *wt.Stream, id int64) {
	c.rw.RLock()
	match := c.id == id && c.stream == stream
	c.rw.RUnlock()
	if match {
		_ = c.forceClose()
	}
}

// doClose tears down the session, waits for the loops and notifies the gate.
func (c *serverConn) doClose(graceful bool) error {
	c.rw.Lock()
	if c.session == nil {
		c.rw.Unlock()
		return errors.ErrConnectionClosed
	}
	c.queue.Close()
	session := c.session
	stream := c.stream
	started := c.started
	c.rw.Unlock()

	if graceful && started {
		c.wgWrite.Wait()
		_ = stream.Close()
		select {
		case <-session.Context().Done():
		case <-time.After(c.mgr.server.opts.closeTimeout):
		}
	}

	err := session.CloseWithError(0, "closed")

	if started {
		c.wgWrite.Wait()
		c.wgRead.Wait()
	}

	for buf := range c.queue.Read() {
		buf.Release()
	}

	if c.mgr.server.disconnectHandler != nil {
		c.mgr.server.disconnectHandler(c)
	}

	// Keep the session address available to the disconnect hook, then release
	// the transport references before returning the connection to the manager.
	c.rw.Lock()
	c.session = nil
	c.stream = nil
	c.rw.Unlock()

	c.mgr.remove(c)

	return err
}

// read decodes ZIM frames from the session stream.
func (c *serverConn) read() {
	var (
		index = 0
		id    = c.id
	)

	c.rw.RLock()
	stream := c.stream
	c.rw.RUnlock()
	if stream == nil {
		return
	}

	for {
		frameType, heartbeatTime, buf, err := packet.Read(stream)
		if err != nil {
			taskpool.Add(func() { c.recycleClose(stream, id) })
			return
		}

		switch c.State() {
		case network.ConnClosed:
			if frameType != packet.FrameHeartbeat {
				buf.Release()
			}
			return
		case network.ConnHanged:
			if frameType != packet.FrameHeartbeat {
				buf.Release()
				return
			}
		}

		if frameType == packet.FrameHeartbeat {
			if c.mgr.server.opts.heartbeatInterval > 0 {
				c.lastHeartbeatTime.Store(time.Now().UnixNano())
			}

			if c.mgr.server.opts.heartbeatMechanism == RespHeartbeat {
				heartbeat := packet.PackHeartbeat(true)
				c.rw.RLock()
				err = c.queue.Write(heartbeat)
				c.rw.RUnlock()
				if err != nil {
					heartbeat.Release()
				}
			}

			if c.mgr.server.heartbeatHandler != nil {
				c.mgr.server.heartbeatHandler(c, heartbeatTime)
			}
			continue
		}

		if c.mgr.server.opts.heartbeatInterval > 0 {
			index++
			if index%10 == 0 {
				c.lastHeartbeatTime.Store(time.Now().UnixNano())
			}
		}

		if buf.Len() == 0 {
			buf.Release()
			continue
		}

		switch {
		case frameType == packet.FrameHandshake && c.mgr.server.handshakeHandler != nil:
			c.mgr.server.handshakeHandler(c, buf)
		case c.mgr.server.receiveHandler != nil:
			c.mgr.server.receiveHandler(c, buf)
		default:
			buf.Release()
		}
	}
}

// write drains the queue and drives the heartbeat timer.
func (c *serverConn) runWrite() {
	defer c.wgWrite.Done()

	var tickerC <-chan time.Time
	var ticker *time.Ticker
	if interval := c.mgr.server.opts.heartbeatInterval; interval > 0 {
		ticker = time.NewTicker(interval)
		tickerC = ticker.C
	}

	c.rw.RLock()
	stream := c.stream
	c.rw.RUnlock()
	if stream == nil {
		if ticker != nil {
			ticker.Stop()
		}
		return
	}

	for {
		select {
		case buf, ok := <-c.queue.Read():
			if !ok {
				if ticker != nil {
					ticker.Stop()
				}
				return
			}
			c.writeFrame(stream, buf)
		case now, ok := <-tickerC:
			if !ok {
				return
			}
			if !c.handleHeartbeat(stream, now) {
				ticker.Stop()
				return
			}
		}
	}
}

// writeFrame writes one frame and releases it.
func (c *serverConn) writeFrame(stream *wt.Stream, buf buffer.Buffer) {
	closeSignal := buf.Len() == 0
	c.queue.Done(closeSignal)
	if closeSignal {
		buf.Release()
		return
	}

	if timeout := c.mgr.server.opts.writeTimeout; timeout > 0 {
		_ = stream.SetWriteDeadline(time.Now().Add(timeout))
	}
	if err := writeBuffer(stream, buf); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Warnf("webtransport write message error: %v", err)
		id := c.id
		taskpool.Add(func() { c.recycleClose(stream, id) })
	}
	buf.Release()
}

// handleHeartbeat closes an idle connection or sends a tick heartbeat.
func (c *serverConn) handleHeartbeat(stream *wt.Stream, now time.Time) bool {
	interval := c.mgr.server.opts.heartbeatInterval
	if c.lastHeartbeatTime.Load() < now.Add(-2*interval).UnixNano() {
		log.Debugf("webtransport connection heartbeat timeout, cid: %d", c.id)
		id := c.id
		taskpool.Add(func() { c.recycleClose(stream, id) })
		return false
	}

	if c.mgr.server.opts.heartbeatMechanism == TickHeartbeat {
		if timeout := c.mgr.server.opts.writeTimeout; timeout > 0 {
			_ = stream.SetWriteDeadline(time.Now().Add(timeout))
		}
		heartbeat := packet.PackHeartbeat(true)
		if err := writeBuffer(stream, heartbeat); err != nil {
			log.Warnf("webtransport write heartbeat error: %v", err)
		}
	}
	return true
}

// writeBuffer writes every fragment of buf and handles short writes.
func writeBuffer(writer *wt.Stream, buf buffer.Buffer) error {
	if buf.Nodes() == 1 {
		return writeAll(writer, buf.Bytes())
	}

	var writeErr error
	buf.VisitBytes(func(part []byte) bool {
		writeErr = writeAll(writer, part)
		return writeErr == nil
	})
	return writeErr
}

// writeAll writes b completely.
func writeAll(writer *wt.Stream, b []byte) error {
	for len(b) > 0 {
		n, err := writer.Write(b)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

// Set stores a connection attribute.
func (a *attr) Set(key, value any) {
	a.values.Store(key, value)
}

// Get returns a connection attribute.
func (a *attr) Get(key any) (any, bool) {
	return a.values.Load(key)
}

// Del deletes a connection attribute.
func (a *attr) Del(key any) bool {
	_, loaded := a.values.LoadAndDelete(key)
	return loaded
}

// Clear removes every connection attribute.
func (a *attr) Clear() {
	a.values.Range(func(key, _ any) bool {
		a.values.Delete(key)
		return true
	})
}

// Visit calls fn for every connection attribute until fn returns false.
func (a *attr) Visit(fn func(key, value any) bool) {
	a.values.Range(func(key, value any) bool {
		return fn(key, value)
	})
}

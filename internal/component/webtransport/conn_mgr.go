package webtransport

import "sync"

// newConnMgr returns a manager bound to one server run.
func newConnMgr(s *server) *connMgr {
	return &connMgr{
		server: s,
		conns:  make(map[int64]*serverConn),
	}
}

// add assigns an ID and registers one session. It fails when the manager is
// closed or the connection limit has been reached.
func (m *connMgr) add(conn *serverConn) bool {
	m.mu.Lock()
	if m.closed || len(m.conns) >= m.server.opts.maxConnNum {
		m.mu.Unlock()
		return false
	}

	conn.id = m.cid.Add(1)
	m.conns[conn.id] = conn
	active := len(m.conns)
	m.mu.Unlock()

	observeSession("accepted")
	observeActive(active)
	return true
}

// remove unregisters one session.
func (m *connMgr) remove(conn *serverConn) {
	m.mu.Lock()
	if _, ok := m.conns[conn.id]; !ok {
		m.mu.Unlock()
		return
	}
	delete(m.conns, conn.id)
	active := len(m.conns)
	m.mu.Unlock()

	observeActive(active)
}

// close marks the manager closed and force-closes every live session.
func (m *connMgr) close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	conns := make([]*serverConn, 0, len(m.conns))
	for id, conn := range m.conns {
		delete(m.conns, id)
		conns = append(conns, conn)
	}
	m.mu.Unlock()

	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Add(1)
		go func(conn *serverConn) {
			defer wg.Done()
			_ = conn.forceClose()
		}(conn)
	}
	wg.Wait()
	observeActive(0)
}

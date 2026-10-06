package pgtest

import (
	"net"
	"net/url"
	"sync"
	"testing"
)

// Proxy forwards TCP connections to a PostgreSQL server and injects faults (LLD §21.1).
type Proxy struct {
	target string
	ln     net.Listener

	mu     sync.Mutex
	mode   proxyMode
	resume chan struct{} // closed when a stall ends
	conns  map[net.Conn]struct{}
}

type proxyMode int

const (
	pass proxyMode = iota
	cut
	stall
)

// NewProxy starts a proxy in front of the database at databaseURL, closed when t ends, and
// returns it with the URL that connects through it.
func NewProxy(t testing.TB, databaseURL string) (*Proxy, string) {
	t.Helper()
	u, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{target: u.Host, ln: ln, resume: make(chan struct{}), conns: map[net.Conn]struct{}{}}
	go p.accept()
	t.Cleanup(p.close)
	u.Host = ln.Addr().String()
	return p, u.String()
}

// Cut resets open connections and refuses new ones, like a stopped server.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.endStall()
	p.mode = cut
	for c := range p.conns {
		_ = c.Close()
	}
}

// Stall stops forwarding without closing anything, like a blackholed network. Bytes sent
// meanwhile are delivered after Restore, as TCP retransmission would.
func (p *Proxy) Stall() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode != stall {
		p.mode, p.resume = stall, make(chan struct{})
	}
}

// Restore forwards traffic again.
func (p *Proxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.endStall()
	p.mode = pass
}

func (p *Proxy) endStall() {
	if p.mode == stall {
		close(p.resume)
	}
}

func (p *Proxy) close() {
	_ = p.ln.Close()
	p.Cut()
}

func (p *Proxy) accept() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.serve(client)
	}
}

func (p *Proxy) serve(client net.Conn) {
	if !p.track(client) {
		_ = client.Close()
		return
	}
	server, err := net.Dial("tcp", p.target)
	if err != nil || !p.track(server) {
		_ = client.Close()
		if server != nil {
			_ = server.Close()
		}
		return
	}
	go p.pump(server, client)
	p.pump(client, server)
}

// track registers c unless the proxy is cut.
func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode == cut {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

// pump copies src to dst, holding data while the proxy is stalled, and closes both when
// either side ends.
func (p *Proxy) pump(dst, src net.Conn) {
	defer func() {
		p.mu.Lock()
		delete(p.conns, dst)
		delete(p.conns, src)
		p.mu.Unlock()
		_ = dst.Close()
		_ = src.Close()
	}()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.awaitForwarding()
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *Proxy) awaitForwarding() {
	for {
		p.mu.Lock()
		mode, resume := p.mode, p.resume
		p.mu.Unlock()
		if mode != stall {
			return
		}
		<-resume
	}
}

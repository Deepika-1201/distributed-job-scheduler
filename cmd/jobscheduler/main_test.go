package main

import (
	"errors"
	"net"
	"testing"
)

func TestAdvertiseAddr(t *testing.T) {
	ipnet := func(s string) *net.IPNet { return &net.IPNet{IP: net.ParseIP(s), Mask: net.CIDRMask(24, 32)} }
	interfaces := func(addrs ...net.Addr) func() ([]net.Addr, error) {
		return func() ([]net.Addr, error) { return addrs, nil }
	}
	wildcard := &net.TCPAddr{IP: net.IPv6unspecified, Port: 7070}
	for name, tc := range map[string]struct {
		listen     net.Addr
		interfaces func() ([]net.Addr, error)
		want       string
	}{
		"a specific listen address is kept": {&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7070},
			interfaces(ipnet("10.0.1.5")), "127.0.0.1:7070"},
		"a wildcard uses the task's address": {wildcard,
			interfaces(ipnet("127.0.0.1"), ipnet("169.254.172.2"), &net.IPNet{IP: net.ParseIP("fe80::1")}, ipnet("10.0.1.5")),
			"10.0.1.5:7070"},
		"no usable interface falls back": {wildcard, interfaces(ipnet("127.0.0.1")), "[::]:7070"},
		"an interface error falls back": {wildcard,
			func() ([]net.Addr, error) { return nil, errors.New("no interfaces") }, "[::]:7070"},
	} {
		if got := advertiseAddr(tc.listen, tc.interfaces); got != tc.want {
			t.Errorf("%s: advertiseAddr = %q, want %q", name, got, tc.want)
		}
	}
}

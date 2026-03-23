package rediscluster

import (
	"context"
	"net"
)

// Addrs returns the host-accessible addresses for all cluster nodes.
func (c *Container) Addrs() []string {
	seen := make(map[string]bool)
	var addrs []string
	for _, hostAddr := range c.portMap {
		if !seen[hostAddr] {
			seen[hostAddr] = true
			addrs = append(addrs, hostAddr)
		}
	}
	return addrs
}

// NewDialer returns a dialer that rewrites internal Docker addresses (announced
// by Redis cluster nodes) to their corresponding host-mapped ports. Pass this
// dialer to your Redis client so it can connect from outside Docker.
func (c *Container) NewDialer() func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if mapped, ok := c.portMap[addr]; ok {
			addr = mapped
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
}

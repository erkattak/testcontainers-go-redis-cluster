package rediscluster

import (
	"context"
	"net"

	"github.com/redis/go-redis/v9"
)

// Addrs returns the host-accessible addresses for all cluster nodes.
func (c *RedisClusterContainer) Addrs() []string {
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

// ClusterOptions returns a *redis.ClusterOptions pre-configured with host-mapped
// addresses and a NAT-rewriting dialer. The dialer intercepts connections to
// internal Docker addresses (announced by Redis cluster nodes) and rewrites
// them to the corresponding host-mapped ports.
func (c *RedisClusterContainer) ClusterOptions() *redis.ClusterOptions {
	dialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		if mapped, ok := c.portMap[addr]; ok {
			addr = mapped
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}

	opts := &redis.ClusterOptions{
		Addrs:        c.Addrs(),
		Dialer:       dialer,
		MaxRedirects: 3,
	}
	if c.opts.password != "" {
		opts.Password = c.opts.password
	}
	return opts
}

// NewClusterClient is a convenience method that returns a connected
// *redis.ClusterClient with the NAT-rewriting dialer pre-configured.
func (c *RedisClusterContainer) NewClusterClient() *redis.ClusterClient {
	return redis.NewClusterClient(c.ClusterOptions())
}

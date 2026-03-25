package rediscluster

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
)

// NodeInfo describes a single Redis cluster node.
type NodeInfo struct {
	Index        int    // 0-based node index (matches StopNode/PauseNode id parameter)
	InternalAddr string // Address announced by Redis (127.0.0.1:700X)
	ExternalAddr string // Host-accessible address (host:mappedPort)
	InitialRole  string // "master" or "replica" based on startup configuration
	CurrentRole  string // "master" or "replica" based on current topology (only set by CurrentNodes)
}

// Nodes returns information about all cluster nodes. Note that the returned
// information reflects the initial cluster configuration at startup. It is not
// topology-aware: if a failover occurs and a replica is promoted to master,
// InitialRole will still report "replica". Query CLUSTER SLOTS or CLUSTER NODES
// via your Redis client if you need current runtime topology.
func (c *Container) Nodes() []NodeInfo {
	nodes := make([]NodeInfo, c.nodeCount)
	for i := range c.nodeCount {
		port := initialPort + i
		internalAddr := fmt.Sprintf("127.0.0.1:%d", port)

		role := "master"
		if i >= c.opts.masters {
			role = "replica"
		}

		nodes[i] = NodeInfo{
			Index:        i,
			InternalAddr: internalAddr,
			ExternalAddr: c.portMap[internalAddr],
			InitialRole:  role,
		}
	}
	return nodes
}

// CurrentNodes queries the cluster for current topology and returns node
// information with CurrentRole populated based on the live cluster state.
// Unlike Nodes(), this method reflects topology changes from failovers.
func (c *Container) CurrentNodes(ctx context.Context) ([]NodeInfo, error) {
	// Try each node until one responds, since some nodes may be down.
	var output []byte
	var lastErr error
	for i := range c.nodeCount {
		args := []string{"redis-cli", "-p", fmt.Sprintf("%d", initialPort+i)}
		if c.opts.password != "" {
			args = append(args, "-a", c.opts.password)
		}
		args = append(args, "CLUSTER", "NODES")

		code, reader, err := c.Exec(ctx, args)
		if err != nil || code != 0 {
			lastErr = fmt.Errorf("node %d: CLUSTER NODES failed", i)
			continue
		}

		output, err = io.ReadAll(reader)
		if err != nil {
			lastErr = fmt.Errorf("node %d: reading output: %w", i, err)
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		return nil, fmt.Errorf("all nodes failed, last error: %w", lastErr)
	}

	// Parse CLUSTER NODES output to get current roles.
	// Format: <id> <ip:port@cport> <flags> ...
	// Flags contain "master" or "slave" (replica).
	roles := make(map[string]string) // internal addr -> role
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		// addr is "ip:port@cport", extract "ip:port"
		addrPart := strings.Split(fields[1], "@")[0]
		flags := fields[2]

		role := "replica"
		if strings.Contains(flags, "master") {
			role = "master"
		}
		roles[addrPart] = role
	}

	// Build NodeInfo slice with current roles.
	nodes := make([]NodeInfo, c.nodeCount)
	for i := range c.nodeCount {
		port := initialPort + i
		internalAddr := fmt.Sprintf("127.0.0.1:%d", port)

		initialRole := "master"
		if i >= c.opts.masters {
			initialRole = "replica"
		}

		nodes[i] = NodeInfo{
			Index:        i,
			InternalAddr: internalAddr,
			ExternalAddr: c.portMap[internalAddr],
			InitialRole:  initialRole,
			CurrentRole:  roles[internalAddr],
		}
	}
	return nodes, nil
}

// AddrMapping returns a map from internal addresses (as announced by Redis)
// to their corresponding host-accessible external addresses.
func (c *Container) AddrMapping() map[string]string {
	m := make(map[string]string, len(c.portMap))
	for k, v := range c.portMap {
		m[k] = v
	}
	return m
}

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

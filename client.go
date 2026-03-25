package rediscluster

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
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

// MasterNodes returns information about current master nodes in the cluster.
// This is a convenience wrapper around CurrentNodes() that filters by CurrentRole.
func (c *Container) MasterNodes(ctx context.Context) ([]NodeInfo, error) {
	nodes, err := c.CurrentNodes(ctx)
	if err != nil {
		return nil, err
	}

	masters := make([]NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		if n.CurrentRole == "master" {
			masters = append(masters, n)
		}
	}
	return masters, nil
}

// ReplicaForMaster returns the node index of a replica that replicates the given
// master node, based on current cluster topology. Returns an error if no replica
// is found or if the master index is invalid.
//
// This is useful for fault injection tests that need to pre-freeze a replica
// before triggering a failover. By freezing the replica first, then stopping
// the master, tests can observe the intermediate state where migration has been
// detected but resubscription is blocked.
func (c *Container) ReplicaForMaster(ctx context.Context, masterIndex int) (int, error) {
	if masterIndex < 0 || masterIndex >= c.nodeCount {
		return -1, fmt.Errorf("master index %d out of range [0, %d)", masterIndex, c.nodeCount)
	}

	// The cluster topology may take a moment to fully propagate after startup.
	// Retry a few times if we can't find a replica.
	var lastErr error
	for attempt := range 10 {
		replicaIdx, err := c.findReplicaForMaster(ctx, masterIndex)
		if err == nil {
			return replicaIdx, nil
		}
		lastErr = err

		// Only retry for "no replica found" errors, not for other errors.
		if !strings.Contains(err.Error(), "no replica found") {
			return -1, err
		}

		if attempt < 9 {
			select {
			case <-ctx.Done():
				return -1, ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	return -1, lastErr
}

func (c *Container) findReplicaForMaster(ctx context.Context, masterIndex int) (int, error) {
	// Get CLUSTER NODES output to find the master's node ID and its replica.
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
		return -1, fmt.Errorf("all nodes failed, last error: %w", lastErr)
	}

	// Parse CLUSTER NODES to build addr->nodeID and nodeID->masterID maps.
	// Format: <id> <ip:port@cport> <flags> <master-id|-> ...
	type nodeEntry struct {
		id       string
		addr     string
		isMaster bool
		masterID string // for replicas, the ID of their master
	}
	entries := make(map[string]nodeEntry) // addr -> entry

	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		// Node IDs are 40-char hex strings. Extract only hex characters to avoid
		// issues with terminal control codes or other garbage in the output.
		nodeID := extractHexID(fields[0])
		addr := strings.Split(fields[1], "@")[0]
		flags := fields[2]
		masterID := extractHexID(fields[3])

		entries[addr] = nodeEntry{
			id:       nodeID,
			addr:     addr,
			isMaster: strings.Contains(flags, "master"),
			masterID: masterID,
		}
	}

	// Build a port -> entry lookup since CLUSTER NODES may use different IPs.
	portEntries := make(map[int]nodeEntry)
	for addr, entry := range entries {
		// Extract port from addr (format: "ip:port")
		parts := strings.Split(addr, ":")
		if len(parts) != 2 {
			continue
		}
		port := 0
		_, err := fmt.Sscanf(parts[1], "%d", &port)
		if err != nil {
			return -1, fmt.Errorf("invalid port in addr %s: %w", addr, err)
		}
		if port >= initialPort && port < initialPort+c.nodeCount {
			portEntries[port] = entry
		}
	}

	// Find the master's node ID.
	masterPort := initialPort + masterIndex
	masterEntry, ok := portEntries[masterPort]
	if !ok {
		return -1, fmt.Errorf("master at index %d (port %d) not found in cluster", masterIndex, masterPort)
	}
	if !masterEntry.isMaster {
		return -1, fmt.Errorf("node at index %d is not a master", masterIndex)
	}

	// Find a replica whose masterID matches.
	for i := range c.nodeCount {
		port := initialPort + i
		entry, ok := portEntries[port]
		if !ok {
			continue
		}
		if !entry.isMaster && entry.masterID == masterEntry.id {
			return i, nil
		}
	}

	return -1, fmt.Errorf("no replica found for master at index %d", masterIndex)
}

// extractHexID extracts a 40-character hex ID from a string, filtering out
// any terminal control codes or other non-hex characters that may appear in
// container exec output. Returns "-" if the input is "-" (for master nodes).
func extractHexID(s string) string {
	s = strings.TrimSpace(s)
	if s == "-" {
		return "-"
	}
	var result strings.Builder
	for _, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') {
			result.WriteRune(r)
		}
	}
	return result.String()
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

// MapAddress translates an internal Redis cluster address (as returned by
// CLUSTER SLOTS or CLUSTER NODES) to the corresponding external host-accessible
// address. Returns the original address and false if no mapping exists.
func (c *Container) MapAddress(addr string) (string, bool) {
	external, ok := c.portMap[addr]
	return external, ok
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

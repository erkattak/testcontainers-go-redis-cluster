package rediscluster

import (
	"fmt"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type options struct {
	masters                      int
	replicasPerMaster            int
	image                        string
	password                     string
	clusterNodeTimeout           time.Duration
	appendOnly                   bool
	maxMemory                    string
	maxMemoryPolicy              string
	logLevel                     string
	clusterAllowReadsWhenDown    bool
	clusterAllowWritesWhenDown   bool
	clusterRequireFullCoverage   bool
	clusterReplicaNoFailover     bool
	clusterMigrationBarrier      *int
	clusterReplicaValidityFactor *int
}

func defaultOptions() options {
	return options{
		masters:                    3,
		replicasPerMaster:          1,
		image:                      "redis:alpine",
		clusterNodeTimeout:         1000 * time.Millisecond,
		clusterRequireFullCoverage: true,
	}
}

// buildNodeConf renders the redis.conf content for a single node.
func (o options) buildNodeConf(port int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "port %d\n", port)
	fmt.Fprintf(&b, "cluster-enabled yes\n")
	fmt.Fprintf(&b, "cluster-config-file /data/nodes-%d.conf\n", port)
	fmt.Fprintf(&b, "cluster-node-timeout %d\n", o.clusterNodeTimeout.Milliseconds())
	fmt.Fprintf(&b, "bind 0.0.0.0\n")
	fmt.Fprintf(&b, "protected-mode no\n")
	fmt.Fprintf(&b, "daemonize no\n")
	fmt.Fprintf(&b, "logfile \"\"\n")

	if o.appendOnly {
		fmt.Fprintf(&b, "appendonly yes\n")
	} else {
		fmt.Fprintf(&b, "appendonly no\n")
	}
	if o.maxMemory != "" {
		fmt.Fprintf(&b, "maxmemory %s\n", o.maxMemory)
	}
	if o.maxMemoryPolicy != "" {
		fmt.Fprintf(&b, "maxmemory-policy %s\n", o.maxMemoryPolicy)
	}
	if o.logLevel != "" {
		fmt.Fprintf(&b, "loglevel %s\n", o.logLevel)
	}
	if o.clusterAllowReadsWhenDown {
		fmt.Fprintf(&b, "cluster-allow-reads-when-down yes\n")
	}
	if o.clusterAllowWritesWhenDown {
		fmt.Fprintf(&b, "cluster-allow-writes-when-down yes\n")
	}
	if !o.clusterRequireFullCoverage {
		fmt.Fprintf(&b, "cluster-require-full-coverage no\n")
	}
	if o.clusterReplicaNoFailover {
		fmt.Fprintf(&b, "cluster-replica-no-failover yes\n")
	}
	if o.clusterMigrationBarrier != nil {
		fmt.Fprintf(&b, "cluster-migration-barrier %d\n", *o.clusterMigrationBarrier)
	}
	if o.clusterReplicaValidityFactor != nil {
		fmt.Fprintf(&b, "cluster-replica-validity-factor %d\n", *o.clusterReplicaValidityFactor)
	}
	if o.password != "" {
		fmt.Fprintf(&b, "requirepass %s\n", o.password)
		fmt.Fprintf(&b, "masterauth %s\n", o.password)
	}
	return b.String()
}

// clusterOption implements testcontainers.ContainerCustomizer and carries
// a mutation function for our internal options struct.
type clusterOption struct {
	apply func(*options)
}

func (o clusterOption) Customize(req *testcontainers.GenericContainerRequest) error {
	// no-op: our options are resolved separately in RunContainer
	return nil
}

// WithMasters sets the number of master nodes. Minimum 3 for a valid Redis cluster.
func WithMasters(n int) testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.masters = n }}
}

// WithReplicasPerMaster sets the number of replicas per master node.
func WithReplicasPerMaster(n int) testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.replicasPerMaster = n }}
}

// WithPassword sets the Redis AUTH password for all cluster nodes.
func WithPassword(password string) testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.password = password }}
}

// WithClusterNodeTimeout sets how long a node must be unreachable before the
// cluster considers it failed and starts a failover. Defaults to 1s, which is
// appropriate for tests. Production clusters typically use 5–15s.
func WithClusterNodeTimeout(d time.Duration) testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.clusterNodeTimeout = d }}
}

// WithAppendOnly enables Redis AOF persistence (appendonly yes).
func WithAppendOnly() testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.appendOnly = true }}
}

// WithMaxMemory sets the maxmemory limit (e.g. "100mb", "1gb").
// When set, WithMaxMemoryPolicy should also be set to control eviction behaviour.
func WithMaxMemory(limit string) testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.maxMemory = limit }}
}

// WithMaxMemoryPolicy sets the maxmemory-policy (e.g. "allkeys-lru", "volatile-ttl").
func WithMaxMemoryPolicy(policy string) testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.maxMemoryPolicy = policy }}
}

// WithLogLevel sets the Redis log level: debug, verbose, notice, or warning.
func WithLogLevel(level string) testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.logLevel = level }}
}

// WithClusterAllowReadsWhenDown allows replica nodes to serve stale reads even when
// the cluster is in a degraded state. Useful for testing read behaviour during
// partial outages.
func WithClusterAllowReadsWhenDown() testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.clusterAllowReadsWhenDown = true }}
}

// WithClusterAllowWritesWhenDown allows nodes to accept writes even when the cluster
// is in a degraded state. Requires Redis 7+.
func WithClusterAllowWritesWhenDown() testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.clusterAllowWritesWhenDown = true }}
}

// WithoutClusterRequireFullCoverage disables the cluster-require-full-coverage check, so the
// cluster continues serving requests for covered slots even when some hash slots
// are unassigned. Useful for testing degraded-cluster scenarios.
func WithoutClusterRequireFullCoverage() testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.clusterRequireFullCoverage = false }}
}

// WithClusterReplicaNoFailover prevents replicas from automatically promoting to master
// when a master fails. Useful for fault-injection tests where failover should be
// triggered manually.
func WithClusterReplicaNoFailover() testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.clusterReplicaNoFailover = true }}
}

// WithClusterMigrationBarrier sets the minimum number of replicas a master must
// retain before one of its replicas can be migrated to an orphaned master
// (cluster-migration-barrier). The Redis default is 1. Setting a higher value
// makes automatic replica migration less aggressive.
func WithClusterMigrationBarrier(n int) testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.clusterMigrationBarrier = &n }}
}

// WithClusterReplicaValidityFactor controls whether a replica that has been disconnected
// from its master for an extended period is still eligible to failover
// (cluster-replica-validity-factor). A value of 0 means replicas are always
// eligible regardless of disconnection time, which is the typical production
// setting for maximising availability.
func WithClusterReplicaValidityFactor(n int) testcontainers.ContainerCustomizer {
	return clusterOption{apply: func(o *options) { o.clusterReplicaValidityFactor = &n }}
}

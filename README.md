# testcontainers-go-redis-cluster

A [testcontainers-go](https://github.com/testcontainers/testcontainers-go) module for running a Redis cluster in tests.

All cluster nodes run as separate `redis-server` processes inside a **single Docker container**, so you get a real Redis cluster without the overhead of multiple containers.

## Requirements

- Go 1.25+
- Docker

## Installation

```bash
go get github.com/erkattak/testcontainers-go-redis-cluster
```

## Usage

```go
import (
    rediscluster "github.com/erkattak/testcontainers-go-redis-cluster"
    "github.com/redis/go-redis/v9"
    "github.com/testcontainers/testcontainers-go"
)

func TestSomething(t *testing.T) {
    cluster, err := rediscluster.Run(t.Context(), "redis:alpine")
    require.NoError(t, err)
    t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

    client := redis.NewClusterClient(&redis.ClusterOptions{
        Addrs:  cluster.Addrs(),
        Dialer: cluster.NewDialer(),
    })
    defer client.Close()

    err = client.Set(t.Context(), "key", "value", 0).Err()
    require.NoError(t, err)
}
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithMasters(n)` | `3` | Number of master nodes (minimum 3) |
| `WithReplicasPerMaster(n)` | `1` | Number of replicas per master |
| `WithPassword(p)` | `""` | AUTH password for all nodes |
| `WithClusterNodeTimeout(d)` | `1s` | How long a node must be unreachable before failover starts |
| `WithAppendOnly()` | disabled | Enable AOF persistence |
| `WithMaxMemory(limit)` | `""` | Max memory per node (e.g. `"100mb"`) |
| `WithMaxMemoryPolicy(policy)` | `""` | Eviction policy (e.g. `"allkeys-lru"`) |
| `WithLogLevel(level)` | `""` | Redis log level: `debug`, `verbose`, `notice`, `warning` |
| `WithAllowReadsWhenDown()` | disabled | Serve stale reads in degraded state |
| `WithAllowWritesWhenDown()` | disabled | Accept writes in degraded state (Redis 7+) |
| `WithoutFullCoverage()` | disabled | Serve covered slots even when some hash slots are unassigned |
| `WithReplicaNoFailover()` | disabled | Prevent automatic replica promotion |

Standard `testcontainers.ContainerCustomizer` options (e.g. `testcontainers.WithLogger`) are also accepted and passed through to the container request.

## Fault injection

`StopNode` and `PauseNode` let you simulate node failures in tests. Both return an idempotent undo function and support optional auto-recovery.

```go
// Stop node 0; it will restart automatically after 3 seconds.
_, err := cluster.StopNode(ctx, 0, 3*time.Second)

// Pause node 0 (SIGSTOP); resume manually.
resumeFn, err := cluster.PauseNode(ctx, 0, 0)
// ... run assertions ...
err = resumeFn()
```

## How it works

### Single-container design

All Redis nodes run as separate processes inside one container. Ports start at 7000 and increment — a 3-master/1-replica cluster uses ports 7000–7005, all exposed and mapped to random host ports.

### NAT-rewriting dialer

Redis cluster nodes announce their internal addresses to clients. Since clients connect from the host, those addresses are unreachable. `NewDialer()` returns a dialer that transparently rewrites internal `containerIP:port` and `127.0.0.1:port` addresses to the corresponding `host:mappedPort` before dialing.

Always pass both `Addrs()` and `Dialer: cluster.NewDialer()` to your Redis client.

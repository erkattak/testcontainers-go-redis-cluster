package rediscluster_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	rediscluster "github.com/erkattak/testcontainers-go-redis-cluster"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestRunContainer_Defaults(t *testing.T) {
	ctx := context.Background()

	cluster, err := rediscluster.RunContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := cluster.NewClusterClient()
	defer func() {
		require.NoError(t, client.Close())
	}()

	pong, err := client.Ping(ctx).Result()
	require.NoError(t, err)
	assert.Equal(t, "PONG", pong)

	info := clusterInfo(t, ctx, client)
	assert.Equal(t, "ok", info["cluster_state"])
	assert.Equal(t, "6", info["cluster_known_nodes"])
}

func TestRunContainer_CustomTopology(t *testing.T) {
	ctx := context.Background()

	cluster, err := rediscluster.RunContainer(ctx,
		rediscluster.WithMasters(3),
		rediscluster.WithReplicasPerMaster(2),
	)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := cluster.NewClusterClient()
	defer func() {
		require.NoError(t, client.Close())
	}()

	info := clusterInfo(t, ctx, client)
	assert.Equal(t, "ok", info["cluster_state"])
	assert.Equal(t, "9", info["cluster_known_nodes"])
}

func TestRunContainer_InvalidMasters(t *testing.T) {
	ctx := context.Background()
	_, err := rediscluster.RunContainer(ctx, rediscluster.WithMasters(2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least 3 masters")
}

func TestParallelIsolation(t *testing.T) {
	for i := range 3 {
		i := i
		t.Run(fmt.Sprintf("cluster-%d", i), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			cluster, err := rediscluster.RunContainer(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

			client := cluster.NewClusterClient()
			defer func() {
				require.NoError(t, client.Close())
			}()

			key := fmt.Sprintf("test-key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			err = client.Set(ctx, key, value, 0).Err()
			require.NoError(t, err)

			got, err := client.Get(ctx, key).Result()
			require.NoError(t, err)
			assert.Equal(t, value, got)
		})
	}
}

func TestStopNode_AutoRestart(t *testing.T) {
	ctx := context.Background()

	cluster, err := rediscluster.RunContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := cluster.NewClusterClient()
	defer func() {
		require.NoError(t, client.Close())
	}()

	err = client.Set(ctx, "persist-key", "hello", 0).Err()
	require.NoError(t, err)

	// Stop node 0 with 3s auto-restart.
	_, err = cluster.StopNode(ctx, 0, 3*time.Second)
	require.NoError(t, err)

	// Cluster should still serve reads (go-redis routes around the failed node).
	got, err := client.Get(ctx, "persist-key").Result()
	require.NoError(t, err)
	assert.Equal(t, "hello", got)

	require.Eventually(t, func() bool {
		return clusterInfo(t, ctx, client)["cluster_state"] == "ok"
	}, 30*time.Second, 1*time.Second)
}

func TestStopNode_ManualRestart(t *testing.T) {
	ctx := context.Background()

	cluster, err := rediscluster.RunContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := cluster.NewClusterClient()
	defer func() {
		require.NoError(t, client.Close())
	}()

	restartFn, err := cluster.StopNode(ctx, 0, 0)
	require.NoError(t, err)

	err = restartFn()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return clusterInfo(t, ctx, client)["cluster_state"] == "ok"
	}, 15*time.Second, 500*time.Millisecond)
}

func TestPauseNode(t *testing.T) {
	ctx := context.Background()

	cluster, err := rediscluster.RunContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := cluster.NewClusterClient()
	defer func() {
		require.NoError(t, client.Close())
	}()

	err = client.Set(ctx, "pause-key", "world", 0).Err()
	require.NoError(t, err)

	resumeFn, err := cluster.PauseNode(ctx, 0, 0)
	require.NoError(t, err)

	// Reads should still succeed (routed to other nodes).
	got, err := client.Get(ctx, "pause-key").Result()
	require.NoError(t, err)
	assert.Equal(t, "world", got)

	err = resumeFn()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return clusterInfo(t, ctx, client)["cluster_state"] == "ok"
	}, 15*time.Second, 500*time.Millisecond)
}

// TestNodeTemporarilyOffline simulates a node going offline for an extended period
// (e.g. being evicted and waiting for a replacement to become ready) while writes
// continue against the remaining cluster, then verifies that all data is intact
// once the node rejoins and the cluster fully heals.
func TestNodeTemporarilyOffline(t *testing.T) {
	ctx := context.Background()

	cluster, err := rediscluster.RunContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := cluster.NewClusterClient()
	defer func() {
		require.NoError(t, client.Close())
	}()

	const keyCount = 20

	// Write an initial data set before any disruption.
	for i := range keyCount {
		err = client.Set(ctx, fmt.Sprintf("before-%d", i), fmt.Sprintf("val-%d", i), 0).Err()
		require.NoError(t, err)
	}

	// Take a node offline.
	restartFn, err := cluster.StopNode(ctx, 0, 0)
	require.NoError(t, err)

	// Wait for the cluster to elect a new master before writing. Until failover
	// completes, writes to the affected slots will fail regardless of slot map state.
	require.Eventually(t, func() bool {
		return clusterInfo(t, ctx, client)["cluster_state"] == "ok"
	}, 15*time.Second, 200*time.Millisecond)

	// The cluster should continue accepting writes while the original node is still down.
	for i := range keyCount {
		err = client.Set(ctx, fmt.Sprintf("during-%d", i), fmt.Sprintf("val-%d", i), 0).Err()
		require.NoError(t, err)
	}

	// Wait a bit to simulate the node taking time to come back.
	time.Sleep(5 * time.Second)

	// Bring the node back online.
	require.NoError(t, restartFn())

	// Wait for the cluster to fully heal before asserting data.
	require.Eventually(t, func() bool {
		return clusterInfo(t, ctx, client)["cluster_state"] == "ok"
	}, 30*time.Second, 500*time.Millisecond)

	// All data written before and during the outage must be readable.
	for i := range keyCount {
		got, err := client.Get(ctx, fmt.Sprintf("before-%d", i)).Result()
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val-%d", i), got)

		got, err = client.Get(ctx, fmt.Sprintf("during-%d", i)).Result()
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val-%d", i), got)
	}
}

// clusterInfo runs CLUSTER INFO and parses the result into a key-value map.
func clusterInfo(t *testing.T, ctx context.Context, client *redis.ClusterClient) map[string]string {
	t.Helper()
	result, err := client.ClusterInfo(ctx).Result()
	require.NoError(t, err)

	info := make(map[string]string)
	for _, line := range strings.Split(result, "\n") {
		line = strings.TrimSpace(line)
		if key, value, ok := strings.Cut(line, ":"); ok {
			info[key] = value
		}
	}
	return info
}

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

const defaultImage = "redis:alpine"

func testOpts() []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		rediscluster.WithoutClusterRequireFullCoverage(),
		rediscluster.WithClusterAllowReadsWhenDown(),
		rediscluster.WithClusterReplicaValidityFactor(0),
		rediscluster.WithClusterNodeTimeout(time.Second),
		rediscluster.WithClusterMigrationBarrier(1),
	}
}

func newClient(t *testing.T, c *rediscluster.Container) *redis.ClusterClient {
	t.Helper()
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:           c.Addrs(),
		Dialer:          c.NewDialer(),
		MaxRedirects:    3,
		MaxRetries:      3,
		DialTimeout:     500 * time.Millisecond,
		ReadTimeout:     500 * time.Millisecond,
		WriteTimeout:    500 * time.Millisecond,
		MinRetryBackoff: 50 * time.Millisecond,
		MaxRetryBackoff: 250 * time.Millisecond,
	})
}

// isClusterOK returns true when the cluster reports cluster_state:ok AND all
// 16384 hash slots are healthy. The slot check is necessary because with
// cluster-require-full-coverage no, Redis reports cluster_state:ok even when
// some slots are unassigned (i.e. before failover completes).
// Safe to use inside require.Eventually (no t.Fatal calls).
func isClusterOK(ctx context.Context, client *redis.ClusterClient) bool {
	result, err := client.ClusterInfo(ctx).Result()
	if err != nil {
		return false
	}
	return strings.Contains(result, "cluster_state:ok") &&
		strings.Contains(result, "cluster_slots_ok:16384")
}

func TestRun_Defaults(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := newClient(t, cluster)
	defer func() {
		require.NoError(t, client.Close())
	}()

	pong, err := client.Ping(t.Context()).Result()
	require.NoError(t, err)
	assert.Equal(t, "PONG", pong)

	info := clusterInfo(t, client)
	assert.Equal(t, "ok", info["cluster_state"])
	assert.Equal(t, "6", info["cluster_known_nodes"])
}

func TestRun_CustomTopology(t *testing.T) {
	opts := append(testOpts(),
		rediscluster.WithMasters(3),
		rediscluster.WithReplicasPerMaster(2),
	)
	cluster, err := rediscluster.Run(t.Context(), defaultImage, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := newClient(t, cluster)
	defer func() {
		require.NoError(t, client.Close())
	}()

	info := clusterInfo(t, client)
	assert.Equal(t, "ok", info["cluster_state"])
	assert.Equal(t, "9", info["cluster_known_nodes"])
}

func TestRun_InvalidMasters(t *testing.T) {
	_, err := rediscluster.Run(t.Context(), defaultImage, rediscluster.WithMasters(2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least 3 masters")
}

func TestParallelIsolation(t *testing.T) {
	for i := range 3 {
		i := i
		t.Run(fmt.Sprintf("cluster-%d", i), func(t *testing.T) {
			t.Parallel()

			cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
			require.NoError(t, err)
			t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

			client := newClient(t, cluster)
			defer func() {
				require.NoError(t, client.Close())
			}()

			key := fmt.Sprintf("test-key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			err = client.Set(t.Context(), key, value, 0).Err()
			require.NoError(t, err)

			got, err := client.Get(t.Context(), key).Result()
			require.NoError(t, err)
			assert.Equal(t, value, got)
		})
	}
}

func TestStopNode_AutoRestart(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := newClient(t, cluster)
	defer func() {
		require.NoError(t, client.Close())
	}()

	err = client.Set(t.Context(), "persist-key", "hello", 0).Err()
	require.NoError(t, err)

	// Stop node 0 with 3s auto-restart.
	_, err = cluster.StopNode(t.Context(), 0, 3*time.Second)
	require.NoError(t, err)

	// Cluster should still serve reads (go-redis routes around the failed node).
	got, err := client.Get(t.Context(), "persist-key").Result()
	require.NoError(t, err)
	assert.Equal(t, "hello", got)

	require.Eventually(t, func() bool {
		return isClusterOK(t.Context(), client)
	}, 15*time.Second, 500*time.Millisecond)
}

func TestStopNode_ManualRestart(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := newClient(t, cluster)
	defer func() {
		require.NoError(t, client.Close())
	}()

	restartFn, err := cluster.StopNode(t.Context(), 0, 0)
	require.NoError(t, err)

	err = restartFn()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return isClusterOK(t.Context(), client)
	}, 10*time.Second, 500*time.Millisecond)
}

func TestPauseNode(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := newClient(t, cluster)
	defer func() {
		require.NoError(t, client.Close())
	}()

	err = client.Set(t.Context(), "pause-key", "world", 0).Err()
	require.NoError(t, err)

	resumeFn, err := cluster.PauseNode(t.Context(), 0, 0)
	require.NoError(t, err)

	// Reads should still succeed (routed to other nodes).
	got, err := client.Get(t.Context(), "pause-key").Result()
	require.NoError(t, err)
	assert.Equal(t, "world", got)

	err = resumeFn()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return isClusterOK(t.Context(), client)
	}, 10*time.Second, 500*time.Millisecond)
}

// TestNodeTemporarilyOffline simulates a node going offline for an extended period
// (e.g. being evicted and waiting for a replacement to become ready) while writes
// continue against the remaining cluster, then verifies that all data is intact
// once the node rejoins and the cluster fully heals.
func TestNodeTemporarilyOffline(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := newClient(t, cluster)
	defer func() {
		require.NoError(t, client.Close())
	}()

	const keyCount = 20

	// Write an initial data set before any disruption.
	for i := range keyCount {
		err = client.Set(t.Context(), fmt.Sprintf("before-%d", i), fmt.Sprintf("val-%d", i), 0).Err()
		require.NoError(t, err)
	}

	// Take a node offline.
	restartFn, err := cluster.StopNode(t.Context(), 0, 0)
	require.NoError(t, err)

	// Wait for the replica to be promoted and all slots to be healthy again.
	// With cluster-node-timeout 1s and replica-validity-factor 0, failover
	// completes so quickly that the transient degraded state (slots_ok < 16384)
	// is not reliably observable via polling.
	require.Eventually(t, func() bool {
		return isClusterOK(t.Context(), client)
	}, 10*time.Second, 200*time.Millisecond)

	// Force the client to refresh its slot map so it learns about the promoted replica.
	client.ReloadState(t.Context())

	// The cluster should continue accepting writes while the original node is still down.
	for i := range keyCount {
		err = client.Set(t.Context(), fmt.Sprintf("during-%d", i), fmt.Sprintf("val-%d", i), 0).Err()
		require.NoError(t, err)
	}

	// Bring the node back online.
	require.NoError(t, restartFn())

	// Wait for the cluster to fully heal before asserting data.
	require.Eventually(t, func() bool {
		return isClusterOK(t.Context(), client)
	}, 15*time.Second, 500*time.Millisecond)

	// All data written before and during the outage must be readable.
	for i := range keyCount {
		got, err := client.Get(t.Context(), fmt.Sprintf("before-%d", i)).Result()
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val-%d", i), got)

		got, err = client.Get(t.Context(), fmt.Sprintf("during-%d", i)).Result()
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val-%d", i), got)
	}
}

func TestNodes(t *testing.T) {
	opts := append(testOpts(),
		rediscluster.WithMasters(3),
		rediscluster.WithReplicasPerMaster(2),
	)
	cluster, err := rediscluster.Run(t.Context(), defaultImage, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	nodes := cluster.Nodes()
	assert.Len(t, nodes, 9) // 3 masters + 6 replicas

	// Verify node indices and roles
	for i, node := range nodes {
		assert.Equal(t, i, node.Index)
		assert.Contains(t, node.InternalAddr, fmt.Sprintf("127.0.0.1:700%d", i))
		assert.NotEmpty(t, node.ExternalAddr)

		if i < 3 {
			assert.Equal(t, "master", node.InitialRole)
		} else {
			assert.Equal(t, "replica", node.InitialRole)
		}
	}

	// Verify external addresses are reachable via the client
	client := newClient(t, cluster)
	defer func() {
		require.NoError(t, client.Close())
	}()

	pong, err := client.Ping(t.Context()).Result()
	require.NoError(t, err)
	assert.Equal(t, "PONG", pong)
}

func TestCurrentNodes(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	// Initially, CurrentRole should match InitialRole
	nodes, err := cluster.CurrentNodes(t.Context())
	require.NoError(t, err)
	assert.Len(t, nodes, 6) // 3 masters + 3 replicas

	masters := 0
	replicas := 0
	for _, node := range nodes {
		assert.Equal(t, node.InitialRole, node.CurrentRole)
		if node.CurrentRole == "master" {
			masters++
		} else {
			replicas++
		}
	}
	assert.Equal(t, 3, masters)
	assert.Equal(t, 3, replicas)
}

func TestCurrentNodes_AfterFailover(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := newClient(t, cluster)
	defer func() {
		require.NoError(t, client.Close())
	}()

	// Stop node 0 (a master) to trigger failover
	_, err = cluster.StopNode(t.Context(), 0, 0)
	require.NoError(t, err)

	// Wait for failover to complete
	require.Eventually(t, func() bool {
		return isClusterOK(t.Context(), client)
	}, 15*time.Second, 500*time.Millisecond)

	// Now check topology - a replica should have been promoted
	nodes, err := cluster.CurrentNodes(t.Context())
	require.NoError(t, err)

	// At least one node should have CurrentRole != InitialRole (the promoted replica)
	promoted := false
	for _, node := range nodes {
		if node.InitialRole == "replica" && node.CurrentRole == "master" {
			promoted = true
			break
		}
	}
	assert.True(t, promoted, "expected a replica to be promoted to master")
}

func TestMasterNodes(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	masters, err := cluster.MasterNodes(t.Context())
	require.NoError(t, err)
	assert.Len(t, masters, 3) // Default: 3 masters

	for _, m := range masters {
		assert.Equal(t, "master", m.CurrentRole)
	}
}

func TestMapAddress(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	// Valid internal address should map
	external, ok := cluster.MapAddress("127.0.0.1:7000")
	assert.True(t, ok)
	assert.NotEmpty(t, external)

	// Unknown address returns false
	_, ok = cluster.MapAddress("unknown:1234")
	assert.False(t, ok)
}

func TestAddrMapping(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	mapping := cluster.AddrMapping()

	// Default config: 3 masters + 3 replicas = 6 nodes
	// Each node has 2 entries (containerIP:port and 127.0.0.1:port)
	assert.Len(t, mapping, 12)

	// Verify 127.0.0.1 entries exist for all ports
	for i := range 6 {
		key := fmt.Sprintf("127.0.0.1:700%d", i)
		assert.Contains(t, mapping, key)
		assert.NotEmpty(t, mapping[key])
	}

	// Verify mutation safety - modifying returned map doesn't affect container
	mapping["test"] = "value"
	mapping2 := cluster.AddrMapping()
	assert.NotContains(t, mapping2, "test")
}

func TestReplicaForMaster(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	// Default: 3 masters (indices 0,1,2) each with 1 replica (indices 3,4,5)
	for masterIdx := range 3 {
		replicaIdx, err := cluster.ReplicaForMaster(t.Context(), masterIdx)
		require.NoError(t, err, "failed for master %d", masterIdx)
		assert.GreaterOrEqual(t, replicaIdx, 3, "replica index should be >= 3")
		assert.Less(t, replicaIdx, 6, "replica index should be < 6")
	}

	// Invalid master index returns error
	_, err = cluster.ReplicaForMaster(t.Context(), 99)
	require.Error(t, err)

	// Replica index (not a master) returns error
	_, err = cluster.ReplicaForMaster(t.Context(), 3)
	require.Error(t, err)
}

// TestPreFreezePattern demonstrates how to use pre-freezing to create a controlled
// timing window during failover. This pattern is useful for testing scenarios that
// require observing intermediate states during migration/resubscription.
//
// The key insight: by freezing the replica BEFORE stopping the master, the client's
// attempt to reconnect to the promoted replica will hang, giving tests time to
// observe the "migration detected but not yet resubscribed" state.
//
// Usage pattern for consumer tests:
//
//	replicaIdx, _ := cluster.ReplicaForMaster(ctx, masterIdx)
//	resume, _ := cluster.PauseNode(ctx, replicaIdx, 0)  // Pre-freeze replica
//	cluster.StopNode(ctx, masterIdx, 0)                  // Trigger failover
//	// ... observe intermediate state, run assertions ...
//	resume()                                             // Allow resubscription
func TestPreFreezePattern(t *testing.T) {
	cluster, err := rediscluster.Run(t.Context(), defaultImage, testOpts()...)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.CleanupContainer(t, cluster) })

	client := newClient(t, cluster)
	defer func() {
		require.NoError(t, client.Close())
	}()

	// Write data across multiple slots before failover.
	for i := range 10 {
		key := fmt.Sprintf("prefreeze-key-%d", i)
		err = client.Set(t.Context(), key, "value", 0).Err()
		require.NoError(t, err)
	}

	// Step 1: Identify which replica will be promoted when master 0 fails.
	replicaIdx, err := cluster.ReplicaForMaster(t.Context(), 0)
	require.NoError(t, err)
	t.Logf("Replica for master 0 is node %d", replicaIdx)

	// Step 2: Pre-freeze the replica. It's still a replica, so the cluster doesn't
	// care that it's frozen. But when promoted, clients won't be able to use slots
	// owned by that master until it's resumed.
	resumeReplica, err := cluster.PauseNode(t.Context(), replicaIdx, 0)
	require.NoError(t, err)

	// Step 3: Stop the master to trigger failover.
	_, err = cluster.StopNode(t.Context(), 0, 0)
	require.NoError(t, err)

	// At this point:
	// - Master 0 is stopped
	// - The cluster will promote the replica
	// - But the promoted replica is frozen (SIGSTOP)
	// - Client attempts to access master 0's former slots will hang or fail
	//
	// This is the window where tests can observe "migration detected, resubscription pending".
	// Consumer tests would add their specific assertions here.

	t.Log("Master stopped and replica frozen - this is the timing window for assertions")

	// Step 4: Resume the replica (now master) to complete resubscription.
	err = resumeReplica()
	require.NoError(t, err)
	t.Log("Replica resumed")

	// Step 5: Verify the cluster recovers and data is accessible.
	require.Eventually(t, func() bool {
		return isClusterOK(t.Context(), client)
	}, 15*time.Second, 500*time.Millisecond)

	client.ReloadState(t.Context())

	// Verify all data is still accessible after recovery.
	for i := range 10 {
		key := fmt.Sprintf("prefreeze-key-%d", i)
		got, err := client.Get(t.Context(), key).Result()
		require.NoError(t, err)
		assert.Equal(t, "value", got)
	}
}

// clusterInfo runs CLUSTER INFO and parses the result into a key-value map.
func clusterInfo(t *testing.T, client *redis.ClusterClient) map[string]string {
	t.Helper()
	result, err := client.ClusterInfo(t.Context()).Result()
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

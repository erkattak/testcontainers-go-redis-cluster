// Package rediscluster provides a testcontainers-go module for running a Redis
// cluster inside a single Docker container. All cluster nodes are started as
// separate redis-server processes within that container and a NAT-rewriting
// dialer is provided so that go-redis clients can connect from the host.
package rediscluster

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const initialPort = 7000

// Container represents a running Redis cluster in a single Docker container.
type Container struct {
	testcontainers.Container
	opts      options
	portMap   map[string]string // "containerIP:7000" -> "host:XXXXX"
	nodeCount int
}

// Run starts a Redis cluster container using the given image and options.
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	o := defaultOptions()
	o.image = img

	var containerCustomizers []testcontainers.ContainerCustomizer
	for _, opt := range opts {
		if co, ok := opt.(clusterOption); ok {
			co.apply(&o)
		} else {
			containerCustomizers = append(containerCustomizers, opt)
		}
	}

	if o.masters < 3 {
		return nil, fmt.Errorf("redis cluster requires at least 3 masters, got %d", o.masters)
	}
	if o.replicasPerMaster < 0 {
		return nil, fmt.Errorf("replicas per master must be >= 0, got %d", o.replicasPerMaster)
	}

	nodeCount := o.masters * (1 + o.replicasPerMaster)

	exposedPorts := make([]string, nodeCount)
	for i := range nodeCount {
		exposedPorts[i] = fmt.Sprintf("%d/tcp", initialPort+i)
	}

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        o.image,
			ExposedPorts: exposedPorts,
			Cmd:          []string{"sleep", "infinity"},
			WaitingFor:   wait.ForExec([]string{"echo", "ready"}),
			HostConfigModifier: func(hc *container.HostConfig) {
				init := true
				hc.Init = &init
			},
		},
		Started: true,
	}

	for _, c := range containerCustomizers {
		if err := c.Customize(&req); err != nil {
			return nil, fmt.Errorf("applying container customizer: %w", err)
		}
	}

	container, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("starting redis cluster container: %w", err)
	}

	if err := startRedisNodes(ctx, container, nodeCount, o); err != nil {
		return nil, fmt.Errorf("starting redis nodes: %w", err)
	}

	if err := formCluster(ctx, container, nodeCount, o); err != nil {
		return nil, fmt.Errorf("forming redis cluster: %w", err)
	}

	containerIP, err := container.ContainerIP(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting container IP: %w", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting container host: %w", err)
	}

	portMap := make(map[string]string, nodeCount*2)
	for i := range nodeCount {
		port := initialPort + i
		mappedPort, err := container.MappedPort(ctx, nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			return nil, fmt.Errorf("getting mapped port for %d: %w", port, err)
		}
		hostAddr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
		// Map both the container IP and loopback, since Redis cluster nodes
		// may announce either address depending on configuration.
		portMap[fmt.Sprintf("%s:%d", containerIP, port)] = hostAddr
		portMap[fmt.Sprintf("127.0.0.1:%d", port)] = hostAddr
	}

	return &Container{
		Container: container,
		opts:      o,
		portMap:   portMap,
		nodeCount: nodeCount,
	}, nil
}

func startRedisNodes(ctx context.Context, container testcontainers.Container, nodeCount int, o options) error {
	if _, _, err := container.Exec(ctx, []string{"mkdir", "-p", "/etc/redis", "/data"}); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	for i := range nodeCount {
		port := initialPort + i
		conf := fmt.Sprintf("/etc/redis/redis-%d.conf", port)

		confContent := o.buildNodeConf(port)

		writeCmd := []string{"sh", "-c", fmt.Sprintf("cat > %s << 'CONF'\n%sCONF", conf, confContent)}
		if _, _, err := container.Exec(ctx, writeCmd); err != nil {
			return fmt.Errorf("writing config for port %d: %w", port, err)
		}

		if _, _, err := container.Exec(ctx, []string{"sh", "-c", fmt.Sprintf("redis-server %s >> /proc/1/fd/1 2>&1 &", conf)}); err != nil {
			return fmt.Errorf("starting redis-server on port %d: %w", port, err)
		}
	}

	// Poll all nodes in parallel to minimise startup time.
	errc := make(chan error, nodeCount)
	for i := range nodeCount {
		port := initialPort + i
		go func() {
			waitCmd := []string{"sh", "-c", fmt.Sprintf(
				"for i in $(seq 1 30); do redis-cli -p %d ping > /dev/null 2>&1 && exit 0; sleep 0.5; done; exit 1", port,
			)}
			code, _, err := container.Exec(ctx, waitCmd)
			if err != nil || code != 0 {
				errc <- fmt.Errorf("redis-server on port %d did not become ready", port)
				return
			}
			errc <- nil
		}()
	}
	for range nodeCount {
		if err := <-errc; err != nil {
			return err
		}
	}

	return nil
}

func formCluster(ctx context.Context, container testcontainers.Container, nodeCount int, o options) error {
	nodes := make([]string, nodeCount)
	for i := range nodeCount {
		nodes[i] = fmt.Sprintf("127.0.0.1:%d", initialPort+i)
	}

	args := []string{"redis-cli", "--cluster", "create"}
	args = append(args, nodes...)
	args = append(args, "--cluster-replicas", fmt.Sprintf("%d", o.replicasPerMaster), "--cluster-yes")
	if o.password != "" {
		args = append(args, "-a", o.password)
	}

	code, _, err := container.Exec(ctx, args)
	if err != nil {
		return fmt.Errorf("redis-cli --cluster create: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("redis-cli --cluster create exited with code %d", code)
	}

	verifyArgs := []string{"redis-cli"}
	if o.password != "" {
		verifyArgs = append(verifyArgs, "-a", o.password)
	}
	verifyArgs = append(verifyArgs, "-p", fmt.Sprintf("%d", initialPort), "cluster", "info")

	for range 30 {
		code, reader, err := container.Exec(ctx, verifyArgs)
		if err == nil && code == 0 && reader != nil {
			b, _ := io.ReadAll(reader)
			if strings.Contains(string(b), "cluster_state:ok") {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("cluster did not reach healthy state")
}

package rediscluster

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// StopNode stops the redis-server process for the given node (0-based index).
// If pendingDuration > 0, the node will automatically restart after that duration.
// The returned restartFn can be called to restart the node immediately;
// it is idempotent — calling it multiple times or after auto-restart is safe.
func (c *Container) StopNode(ctx context.Context, id int, pendingDuration time.Duration) (restartFn func() error, err error) {
	if id < 0 || id >= c.nodeCount {
		return nil, fmt.Errorf("node id %d out of range [0, %d)", id, c.nodeCount)
	}

	port := initialPort + id
	shutdownCmd := []string{"redis-cli", "-p", fmt.Sprintf("%d", port)}
	if c.opts.password != "" {
		shutdownCmd = append(shutdownCmd, "-a", c.opts.password)
	}
	shutdownCmd = append(shutdownCmd, "shutdown", "nosave")

	_, _, err = c.Exec(ctx, shutdownCmd)
	if err != nil {
		return nil, fmt.Errorf("stopping node %d (port %d): %w", id, port, err)
	}

	conf := fmt.Sprintf("/etc/redis/redis-%d.conf", port)
	return onceFunc(func() error {
		_, _, err := c.Exec(context.Background(), []string{"sh", "-c", fmt.Sprintf("redis-server %s >> /proc/1/fd/1 2>&1 &", conf)})
		return err
	}, pendingDuration), nil
}

// PauseNode sends SIGSTOP to the redis-server process for the given node (0-based index),
// freezing it without closing connections. If pendingDuration > 0, the node will
// automatically resume after that duration. The returned resumeFn can be called to
// resume immediately; it is idempotent.
func (c *Container) PauseNode(ctx context.Context, id int, pendingDuration time.Duration) (resumeFn func() error, err error) {
	if id < 0 || id >= c.nodeCount {
		return nil, fmt.Errorf("node id %d out of range [0, %d)", id, c.nodeCount)
	}

	port := initialPort + id
	pid, err := c.redisPID(ctx, port)
	if err != nil {
		return nil, fmt.Errorf("getting PID for node %d (port %d): %w", id, port, err)
	}

	_, _, err = c.Exec(ctx, []string{"kill", "-STOP", pid})
	if err != nil {
		return nil, fmt.Errorf("pausing node %d (pid %s): %w", id, pid, err)
	}

	return onceFunc(func() error {
		_, _, err := c.Exec(context.Background(), []string{"kill", "-CONT", pid})
		return err
	}, pendingDuration), nil
}

// onceFunc returns an idempotent function that calls fn at most once.
// If autoDuration > 0, fn is also scheduled automatically via time.AfterFunc.
func onceFunc(fn func() error, autoDuration time.Duration) func() error {
	var once sync.Once
	f := func() error {
		var err error
		once.Do(func() { err = fn() })
		return err
	}
	if autoDuration > 0 {
		time.AfterFunc(autoDuration, func() { _ = f() })
	}
	return f
}

// redisPID returns the PID of the redis-server process listening on the given port.
func (c *Container) redisPID(ctx context.Context, port int) (string, error) {
	args := []string{"sh", "-c", fmt.Sprintf("redis-cli -p %d info server | grep process_id | cut -d: -f2 | tr -d '\\r'", port)}
	_, reader, err := c.Exec(ctx, args)
	if err != nil {
		return "", err
	}
	b, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("reading PID output: %w", err)
	}
	// Extract only numeric characters from the output.
	// Docker exec output may contain multiplexed stream headers (binary garbage)
	// that need to be stripped to get the actual PID value.
	pid := extractNumericPID(string(b))
	if pid == "" || pid == "0" {
		return "", fmt.Errorf("could not find PID for redis-server on port %d", port)
	}
	return pid, nil
}

// extractNumericPID extracts only numeric characters from the output,
// filtering out any Docker exec multiplexed stream headers.
func extractNumericPID(s string) string {
	var result strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

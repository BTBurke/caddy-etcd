package etcd

import (
	"io/ioutil"
	"os"
	"strings"
	"time"
)

type ClusterConfig struct {
	KeyPrefix string
	ServerIP []string
	LockTimeout time.Duration
	CaddyFile []byte
}

type ConfigOption func(c *ClusterConfig) error

func NewClusterConfig(opts ...ConfigOption) (*ClusterConfig, error) {
	c := &ClusterConfig {
		KeyPrefix: "/caddy",
		LockTimeout: time.Duration(5 * time.Minute),
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	if len(c.ServerIP) == 0 {
		c.ServerIP = []string{"127.0.0.1:2379"}
	}

	if len(c.CaddyFile) == 0 {

	}
	return c, nil
}

func ConfigOptsFromEnvironment() (opts []ConfigOption) {
	var env = map[string]func(s string) ConfigOption {
		"CLUSTER_ETCD_SERVER": WithServers,
		"CLUSTER_ETCD_PREFIX": WithPrefix,
		"CLUSTER_ETCD_LOCK_TIMEOUT": WithTimeout,
		"CLUSTER_ETCD_CADDYFILE": WithCaddyFile,
	}
	for e, f := range env {
		val := os.Getenv(e)
		if len(e) != 0 {
			opts = append(opts, f(val))
		}
	}
	return opts
}

func WithServers(s string) ConfigOption {
	return func(c *ClusterConfig) error {
		srvs := strings.Split(s, ",")
		c.ServerIP = srvs
		return nil
	}
}

func WithPrefix(s string) ConfigOption {
	return func(c *ClusterConfig) error {
		c.KeyPrefix = "/" + strings.Trim(s, "/")
		return nil
	}
}

func WithTimeout(s string) ConfigOption {
	return func(c *ClusterConfig) error {
		d, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		c.LockTimeout = d
		return nil
	}
}

func WithCaddyFile(s string) ConfigOption {
	return func(c *ClusterConfig) error {
		r, err := ioutil.ReadFile(s)
		if err != nil {
			return err
		}
		c.CaddyFile = r
		return nil
	}
}
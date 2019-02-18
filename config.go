package etcd

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// ClusterConfig maintains configuration information for cluster
// resources such as etcd server instances
type ClusterConfig struct {
	KeyPrefix        string
	ServerIP         []string
	LockTimeout      time.Duration
	CaddyFile        []byte
	CaddyFilePath    string
	DisableCaddyLoad bool
	// TODO: Add roles, auth, and mutual TLS
}

// ConfigOption represents a functional option for ClusterConfig
type ConfigOption func(c *ClusterConfig) error

// NewClusterConfig returns a new configuration with options passed as functional
// options
func NewClusterConfig(opts ...ConfigOption) (*ClusterConfig, error) {
	c := &ClusterConfig{
		KeyPrefix:   "/caddy",
		LockTimeout: 5 * time.Minute,
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	if len(c.ServerIP) == 0 {
		c.ServerIP = []string{"http://127.0.0.1:2379"}
	}

	if len(c.CaddyFile) == 0 {

	}
	return c, nil
}

// ConfigOptsFromEnvironment reads environment variables and returns options that can be applied via
// NewClusterConfig
func ConfigOptsFromEnvironment() (opts []ConfigOption) {
	var env = map[string]func(s string) ConfigOption{
		"CADDY_CLUSTERING_ETCD_SERVERS":          WithServers,
		"CADDY_CLUSTERING_ETCD_PREFIX":           WithPrefix,
		"CADDY_CLUSTERING_ETCD_TIMEOUT":          WithTimeout,
		"CADDY_CLUSTERING_ETCD_CADDYFILE":        WithCaddyFile,
		"CADDY_CLUSTERING_ETCD_CADDYFILE_LOADER": WithDisableCaddyfileLoad,
	}
	for e, f := range env {
		val := os.Getenv(e)
		if len(val) != 0 {
			opts = append(opts, f(val))
		}
	}
	return opts
}

// WithServers sets the etcd server endpoints.  Multiple endpoints are assumed to
// be separated by a comma, and consist of a full URL, including scheme and port
// (i.e., http://127.0.0.1:2379)  The default config uses port 2379 on localhost.
func WithServers(s string) ConfigOption {
	return func(c *ClusterConfig) error {
		var srvs []string
		switch {
		case strings.Index(s, ";") >= 0:
			srvs = strings.Split(s, ";")
		default:
			srvs = strings.Split(s, ",")
		}
		for _, srv := range srvs {
			csrv := strings.TrimSpace(srv)
			u, err := url.Parse(csrv)
			if err != nil {
				return errors.Wrap(err, "CADDY_CLUSTERING_ETCD_SERVERS is an invalid format: servers should be separated by comma and be a full URL including scheme")
			}
			if u.Scheme != "http" && u.Scheme != "https" {
				return errors.New("CADDY_CLUSTERING_ETCD_SERVERS is an invalid format: servers must specify a scheme, either http or https")
			}
			c.ServerIP = append(c.ServerIP, csrv)
		}
		return nil
	}
}

// WithPrefix sets the etcd namespace for caddy data.  Default is `/caddy`.
// Prefixes are normalized to use `/` as a path separator.
func WithPrefix(s string) ConfigOption {
	return func(c *ClusterConfig) error {
		c.KeyPrefix = path.Clean("/" + strings.Trim(strings.Replace(s, "\\", "/", -1), "/"))
		return nil
	}
}

// WithTimeout sets the time locks should be considered abandoned.  Locks that
// exist longer than this setting will be overwritten by the next client that
// acquires the lock.  The default is 5 minutes.  This option takes standard
// Go duration formats such as 5m, 1h, etc.
func WithTimeout(s string) ConfigOption {
	return func(c *ClusterConfig) error {
		d, err := time.ParseDuration(s)
		if err != nil {
			return errors.Wrap(err, "CADDY_CLUSTERING_ETCD_TIMEOUT is an invalid format: must be a go standard time duration")
		}
		c.LockTimeout = d
		return nil
	}
}

// WithCaddyFile sets the path to the bootstrap Caddyfile to load on initial start if configuration
// information is not already present in etcd.  The first cluster instance will load this
// file and store it in etcd.  Subsequent members of the cluster will prioritize configuration
// from etcd even if this file is present.  This function will not error even if the Caddyfile is
// not present.  If a caddyfile cannot be read from etcd, from this file, or from the default loader,
// caddy will start with an empty default configuration.
func WithCaddyFile(s string) ConfigOption {
	return func(c *ClusterConfig) error {
		p := path.Clean(s)
		if !strings.HasPrefix(p, "/") {
			// assume a relative directory
			cwd, err := os.Getwd()
			if err != nil {
				log.Print("[WARN] etcd: could not ready configured caddyfile source, this may not indicate a problem if another cluster member has stored this data in etcd")
				return nil
			}
			p = path.Join(cwd, p)
		}
		r, err := ioutil.ReadFile(p)
		if err != nil {
			log.Print("[WARN] etcd: could not ready configured caddyfile source, this may not indicate a problem if another cluster member has stored this data in etcd")
			return nil
		}
		c.CaddyFilePath = p
		c.CaddyFile = r
		return nil
	}
}

// WithDisableCaddyfileLoad will skip all attempts at loading the caddyfile from etcd and force caddy to fall back
// to other enabled caddyfile loader plugins or the default loader
func WithDisableCaddyfileLoad(s string) ConfigOption {
	return func(c *ClusterConfig) error {
		val := strings.ToLower(strings.TrimSpace(s))
		switch val {
		case "disable":
			c.DisableCaddyLoad = true
			return nil
		case "enable", "":
			return nil
		default:
			return errors.New(fmt.Sprintf("CADDY_CLUSTERING_ETCD_CADDYFILE_LOADER is an invalid format: %s is an unknown option", val))
		}
	}
}

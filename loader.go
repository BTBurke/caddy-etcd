package etcd

import (
	"bytes"
	"path"

	"github.com/cenkalti/backoff"
	"github.com/mholt/caddy"
	"github.com/pkg/errors"
)

var _ caddy.Input = loader{}

// Load satisfies the caddy.Input interface to return the contents of a Caddyfile in the following order:
// (1) any caddy files that are loaded in etcd at key: /<keyprefix>/caddyfile
// (2) a caddyfile that is set using CADDY_CLUSTERING_ETCD_CADDYFILE
// (3) other configured caddyfile loaders, including the default loader
func Load(servertype string) (caddy.Input, error) {
	opts := ConfigOptsFromEnvironment()
	c, err := NewClusterConfig(opts...)
	if err != nil {
		return nil, err
	}
	// Disables loading caddyfile from etcd and passes responsibility for finding a caddyfile to
	// other plugins or the default loader
	if c.DisableCaddyLoad {
		return nil, nil
	}
	cli, err := getClient(c)
	if err != nil {
		return nil, errors.Wrap(err, "caddyfile loader: unable to get etcd client")
	}
	dst := new(bytes.Buffer)
	if err := backoff.Retry(get(cli, path.Join(c.KeyPrefix, "caddyfile"), dst), backoff.NewExponentialBackOff()); err != nil {
		return nil, errors.Wrap(err, "caddyfile loader: unable to load caddyfile from etcd")
	}
	switch {
	// prioritize data loaded in etcd for caddyfile
	case len(dst.Bytes()) > 0:
		return newLoader(dst.Bytes(), path.Join(c.KeyPrefix, "caddyfile"), servertype)
	// fall back to the data in the read from the configured caddyfile, save to etcd for other cluster members
	case len(c.CaddyFile) > 0:
		p := path.Join(c.KeyPrefix, "caddyfile")
		srv := NewService(c)
		if err := srv.Lock("caddyfile"); err != nil {
			// cant get lock, might be race by other clustered etcd instances saving a caddyfile so give up saving it
			// and assume that it should start with the existing configured caddyfile
			return newLoader(c.CaddyFile, c.CaddyFilePath, servertype)
		}
		defer srv.Unlock("caddyfile")
		if err := pipeline(tx(set(cli, p, c.CaddyFile)), nil, backoff.NewExponentialBackOff()); err != nil {
			return nil, errors.Wrap(err, "caddyfile loader: unable to store caddyfile data in etcd")
		}
		return newLoader(c.CaddyFile, c.CaddyFilePath, servertype)
	// pass to the next caddyfile loader
	default:
		return nil, nil
	}

}

type loader struct {
	body       []byte
	path       string
	servertype string
}

func newLoader(body []byte, path string, servertype string) (caddy.Input, error) {
	return loader{
		body:       body,
		path:       path,
		servertype: servertype,
	}, nil
}

func (l loader) Body() []byte {
	return l.body
}

func (l loader) Path() string {
	return l.path
}

func (l loader) ServerType() string {
	return l.servertype
}

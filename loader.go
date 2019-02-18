package etcd

import (
	"bytes"
	"path"

	"github.com/cenkalti/backoff"
	"github.com/mholt/caddy"
	"github.com/pkg/errors"
)

var _ caddy.Input = loader{}

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
		if err := backoff.Retry(set(cli, path.Join(c.KeyPrefix, "caddyfile"), c.CaddyFile), backoff.NewExponentialBackOff()); err != nil {
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

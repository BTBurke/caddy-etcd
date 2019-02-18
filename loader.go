package etcd

import (
	"github.com/mholt/caddy"
)

var _ caddy.Input = loader{}

func Load(servertype string) (caddy.Input, error) {
	// TODO: add lookups for existing caddyfile in etcd, then fallback to bootstrap file, return nil if nothing
	return newLoader(c.Body, c.Path, servertype)
}

type loader struct {
	body []byte
	path string
	servertype string
}

func newLoader(body []byte, path string, servertype string) (caddy.Input, error) {
	return loader{
		body: body,
		path: path,
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
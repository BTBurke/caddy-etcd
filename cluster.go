package etcd

import "github.com/mholt/certmagic"

type Cluster struct {
	srv Service
}

func NewCluster() (*Cluster, error) {
	opts := ConfigOptsFromEnvironment()
	c, err := NewClusterConfig(opts...)
	if err != nil {
		return nil, err
	}
	return &Cluster{
		srv: NewService(c),
	}, nil
}

func (c *Cluster) Lock(key string) error {
	return c.srv.Lock(key)
}

func (c *Cluster) Unlock(key string) error {
	return c.srv.Unlock(key)
}

func (c *Cluster) Store(key string, value []byte) error {
	return c.srv.Store(key, value)
}

func (c *Cluster) Load(key string) ([]byte, error) {
	return c.srv.Load(key)
}

func (c *Cluster) Exists(key string) bool {
	_, err := c.srv.Metadata(key)
	switch {
	case err == nil:
		return true
	case IsNotExistError(err):
		return false
	default:
		return false
	}
}

func (c *Cluster) Delete(key string) error {
	return c.srv.Delete(key)
}

func (c *Cluster) List(prefix string, recursive bool) ([]string, error) {
	switch {
	case recursive:
		return c.srv.List(prefix, FilterRemoveDirectories())
	default:
		return c.srv.List(prefix, FilterExactPrefix(prefix, c.srv.prefix()))
	}
}

func (c *Cluster) Stat(key string) (certmagic.KeyInfo, error) {
	return certmagic.KeyInfo{}, nil
}

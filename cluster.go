package etcd

import (
	"github.com/mholt/caddy/caddytls"
	"github.com/mholt/certmagic"
	"log"
)

// ensure that cluster implements certmagic.Storage
var _ certmagic.Storage = Cluster{}

// register plugin
func init() {
	caddytls.RegisterClusterPlugin("etcd", NewCluster)
}

// Cluster implements the certmagic.Storage interface as a cluster plugin
type Cluster struct {
	srv Service
}

// NewCluster returns a cluster plugin that reads from the environment to configure itself
func NewCluster() (certmagic.Storage, error) {
	log.Printf("Activating etcd clustering")
	opts := ConfigOptsFromEnvironment()
	c, err := NewClusterConfig(opts...)
	if err != nil {
		return Cluster{}, err
	}
	return Cluster{
		srv: NewService(c),
	}, nil
}

// Lock fulfills the certmagic.Storage Locker interface.  Each etcd operation gets a lock
// scoped to the key it is updating with a customizable timeout.  Locks that persist past
// the timeout are assumed to be abandoned.
func (c Cluster) Lock(key string) error {
	return c.srv.Lock(key)
}

// Unlock fulfills the certmagic.Storage Locker interface.  Locks are cleared on a per
// path basis.
func (c Cluster) Unlock(key string) error {
	return c.srv.Unlock(key)
}

// Store fulfills the certmagic.Storage interface.  Each storage operation results in two nodes
// added to etcd.  A node is created for the value of the file being stored.  A matching metadata
// node is created to keep details of creation time, SHA1 hash, and size of the node.  Failures to create
// both nodes in a single transaction make a best effort at restoring the pre-transaction state.
func (c Cluster) Store(key string, value []byte) error {
	return c.srv.Store(key, value)
}

// Load fulfills the certmagic.Storage interface.  Each load operation retrieves the value associated
// at the file node and checks it against the hash stored in the metadata node associated with the file.
// If the node does not exist, a `NotExist` error is returned.  Data corruption found via a hash mismatch
// returns a `FailedChecksum` error.
func (c Cluster) Load(key string) ([]byte, error) {
	return c.srv.Load(key)
}

// Exists fulfills the certmagic.Storage interface.  Exists returns true only if the there is a terminal
// node that exists which represents a file in a filesystem.
func (c Cluster) Exists(key string) bool {
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

// Delete fulfills the certmagic.Storage interface and deletes the node located at key along with any
// associated metadata.
func (c Cluster) Delete(key string) error {
	return c.srv.Delete(key)
}

// List fulfills the certmagic.Storage interface and lists all nodes that exist under path `prefix`.  For
// recursive queries, it returns all keys located at subdirectories of `prefix`.  Otherwise, it only returns
// terminal nodes that represent files present at exactly the patch `prefix`.
func (c Cluster) List(prefix string, recursive bool) ([]string, error) {
	switch {
	case recursive:
		return c.srv.List(prefix, FilterRemoveDirectories())
	default:
		return c.srv.List(prefix, FilterExactPrefix(prefix, c.srv.prefix()))
	}
}

// Stat fulfills the certmagic.Storage interface and returns metadata about existing nodes.  When the
// key represents a file in the filesystem, it returns metadata about the file.  For directories, it traverses
// all children to determine directory size and modified time.
func (c Cluster) Stat(key string) (certmagic.KeyInfo, error) {
	md, err := c.srv.Metadata(key)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}
	return certmagic.KeyInfo{
		Key:        md.Path,
		Modified:   md.Timestamp,
		Size:       int64(md.Size),
		IsTerminal: !md.IsDir,
	}, nil
}

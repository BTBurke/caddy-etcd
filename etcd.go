package etcd

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"log"
	"path"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/client"
)

// token is a random value used to manage locks
var token string

func init() {
	tok := make([]byte, 32)
	_, err := rand.Read(tok)
	if err != nil {
		log.Fatal(err)
	}
	token = base64.StdEncoding.EncodeToString(tok)
}

// Lock is a clients lock on updating keys.  When the same client requests multiple locks, the
// lock is extended.  Assumes that one client does not try to set the same key from different
// go routines.  In this case, a race condition exists and last write wins.
type Lock struct {
	Token    string
	Obtained string
	Key      string
}

// Metadata stores information about a particular node that represents a file in etcd
type Metadata struct {
	Path      string
	Size      int
	Timestamp time.Time
	Hash      [20]byte
	IsDir     bool
}

// NewMetadata returns a metadata information given a path and a file to be stored at the path.
// Typically, one metadata node is stored for each file node in etcd.
func NewMetadata(key string, data []byte) Metadata {
	return Metadata{
		Path:      key,
		Size:      len(data),
		Timestamp: time.Now().UTC(),
		Hash:      sha1.Sum(data),
	}
}

// Service is a low level interface that stores and loads values in Etcd
type Service interface {
	Store(key string, value []byte) error
	Load(key string) ([]byte, error)
	Delete(key string) error
	Metadata(key string) (*Metadata, error)
	Lock(key string) error
	Unlock(key string) error
	List(path string, filters ...func(client.Node) bool) ([]string, error)
	prefix() string
}

type etcdsrv struct {
	mdPrefix string
	lockKey  string
	cfg      *ClusterConfig
	// set noBackoff to true to disable exponential backoff retries
	noBackoff bool
}

// NewService returns a new low level service to store and load values in etcd.  The service is designed to store values with
// associated metadata in a format that allows it to fulfill with the Certmagic storage interface, effectively implementing simple
// filesystem semantics on top of etcd key/value storage.  Locks are acquired before writes to etcd and the library will make its
// best attempt at rolling back transactions that fail.  Concurrent writes are blocking with exponential backoff up to a reasonable
// time limit.  Errors are logged, but do not guarantee that the system will return to a coherent pre-transaction state in the
// presence of significant etcd failures or prolonged unavailability.
func NewService(c *ClusterConfig) Service {
	return &etcdsrv{
		mdPrefix: path.Join(c.KeyPrefix + "/md"),
		lockKey:  path.Join(c.KeyPrefix, "/lock"),
		cfg:      c,
	}
}

// Lock acquires a lock with a maximum lifetime specified by the ClusterConfig
func (e *etcdsrv) Lock(key string) error {
	return e.lock(token, key)
}

// Lock acquires a lock with a maximum lifetime specified by the ClusterConfig
func (e *etcdsrv) lock(tok string, key string) error {
	c, err := getClient(e.cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create etcd client while getting lock")
	}
	acquire := func() error {
		var okToSet bool
		resp, err := c.Get(context.Background(), path.Join(e.lockKey, key), nil)
		if err != nil {
			switch {
			// no existing lock
			case client.IsKeyNotFound(err):
				okToSet = true
				break
			default:
				return errors.Wrap(err, "lock: failed to get existing lock")
			}
		}
		if resp != nil {
			var l Lock
			b, err := base64.StdEncoding.DecodeString(resp.Node.Value)
			if err != nil {
				return errors.Wrap(err, "lock: failed to decode base64 lock representation")
			}
			if err := json.Unmarshal(b, &l); err != nil {
				return errors.Wrap(err, "lock: failed to unmarshal existing lock")
			}
			var lockTime time.Time
			if err := lockTime.UnmarshalText([]byte(l.Obtained)); err != nil {
				return errors.Wrap(err, "lock: failed to unmarshal time")
			}
			switch {
			// lock request from same client extend existing lock
			case l.Token == tok:
				okToSet = true
				break
			// orphaned locks that are past lock timeout allow new lock
			case time.Now().UTC().Sub(lockTime) >= e.cfg.LockTimeout:
				okToSet = true
				break
			default:
			}
		}
		if okToSet {
			now, err := time.Now().UTC().MarshalText()
			if err != nil {
				return errors.Wrap(err, "lock: failed to marshal current UTC time")
			}
			l := Lock{
				Token:    tok,
				Obtained: string(now),
				Key:      key,
			}
			b, err := json.Marshal(l)
			if err != nil {
				return errors.Wrap(err, "lock: failed to marshal new lock")
			}
			if _, err := c.Set(context.Background(), path.Join(e.lockKey, key), base64.StdEncoding.EncodeToString(b), nil); err != nil {
				return errors.Wrap(err, "failed to get lock")
			}
			return nil
		}
		return errors.New("lock: failed to obtain lock, already exists")
	}
	return e.execute(acquire)
}

// Unlock releases the current lock
func (e *etcdsrv) Unlock(key string) error {
	c, err := getClient(e.cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create etcd client while getting lock")
	}
	release := func() error {
		if _, err := c.Delete(context.Background(), path.Join(e.lockKey, key), nil); err != nil {
			return errors.Wrap(err, "failed to release lock")
		}
		return nil
	}
	return e.execute(release)
}

// execute will use exponential backoff when configured
func (e *etcdsrv) execute(o backoff.Operation) error {
	switch e.noBackoff {
	case true:
		return o()
	default:
		return backoff.Retry(o, backoff.NewExponentialBackOff())
	}
}

// Store stores a value at key. This function attempts to rollback to a prior value
// if there is an error in the transaction.
func (e *etcdsrv) Store(key string, value []byte) error {
	cli, err := getClient(e.cfg)
	if err != nil {
		return errors.Wrap(err, "store: failed to get client")
	}
	storageKey := path.Join(e.cfg.KeyPrefix, key)
	storageKeyMD := path.Join(e.mdPrefix, key)
	md := NewMetadata(key, value)

	ex := new(bool)
	if err := e.execute(exists(cli, storageKeyMD, ex)); err != nil {
		return errors.Wrap(err, "store: failed to get old metadata")
	}
	var commits []backoff.Operation
	var rollbacks []backoff.Operation
	switch *ex {
	case true:
		mdPrev := new(Metadata)
		valPrev := new(bytes.Buffer)
		commits = tx(get(cli, storageKey, valPrev), getMD(cli, storageKeyMD, mdPrev), set(cli, storageKey, value), setMD(cli, storageKeyMD, md))
		rollbacks = tx(noop(), noop(), set(cli, storageKey, valPrev.Bytes()), setMD(cli, storageKeyMD, *mdPrev))
	default:
		commits = tx(set(cli, storageKey, value), setMD(cli, storageKeyMD, md))
		rollbacks = tx(del(cli, storageKey), del(cli, storageKeyMD))
	}
	return pipeline(commits, rollbacks, backoff.NewExponentialBackOff())
}

// Load will load the value at key.  If the key does not exist, `NotExist` error is returned.
// Checksums of the value loaded are checked against the SHA1 hash in the metadata.  If they do not
// match, a `FailedChecksum` error is returned.
func (e *etcdsrv) Load(key string) ([]byte, error) {
	cli, err := getClient(e.cfg)
	if err != nil {
		return nil, errors.Wrap(err, "load: failed to get client")
	}
	storageKey := path.Join(e.cfg.KeyPrefix, key)
	storageKeyMD := path.Join(e.mdPrefix, key)
	ex := new(bool)
	if err := e.execute(exists(cli, storageKeyMD, ex)); err != nil {
		return nil, errors.Wrap(err, "load: could not get existence of key")
	}
	switch *ex {
	case false:
		return nil, NotExist{key}
	default:
	}
	md := new(Metadata)
	if err := e.execute(getMD(cli, storageKeyMD, md)); err != nil {
		return nil, errors.Wrap(err, "load: could not get metadata")
	}
	dst := new(bytes.Buffer)
	if err := e.execute(get(cli, storageKey, dst)); err != nil {
		return nil, errors.Wrap(err, "load: could not get data")
	}
	value := dst.Bytes()
	if sha1.Sum(value) != md.Hash {
		return nil, FailedChecksum{key}
	}
	return value, nil
}

// Delete will remove nodes associated with the file at key
func (e *etcdsrv) Delete(key string) error {
	cli, err := getClient(e.cfg)
	if err != nil {
		return errors.Wrap(err, "load: failed to get client")
	}
	storageKey := path.Join(e.cfg.KeyPrefix, key)
	storageKeyMD := path.Join(e.mdPrefix, key)
	commits := tx(del(cli, storageKey), del(cli, storageKeyMD))
	return pipeline(commits, nil, backoff.NewExponentialBackOff())
}

// Metadata will load the metadata associated with the data at node key.  If the
// node does not exist, a `NotExist` error is returned and the metadata will be nil.
func (e *etcdsrv) Metadata(key string) (*Metadata, error) {
	cli, err := getClient(e.cfg)
	if err != nil {
		return nil, errors.Wrap(err, "load: failed to get client")
	}
	storageKeyMD := path.Join(e.mdPrefix, key)
	ex := new(bool)
	if err := e.execute(exists(cli, storageKeyMD, ex)); err != nil {
		return nil, errors.Wrap(err, "load: could not get existence of key")
	}
	switch *ex {
	case false:
		return nil, NotExist{key}
	default:
	}
	md := new(Metadata)
	if err := e.execute(getMD(cli, storageKeyMD, md)); err != nil {
		return nil, errors.Wrap(err, "load: could not get metadata")
	}
	// directory virtual nodes need to remove the MD prefix
	if md.IsDir {
		md.Path = strings.TrimPrefix(md.Path, e.mdPrefix)
	}
	return md, nil
}

func (e *etcdsrv) List(key string, filters ...func(client.Node) bool) ([]string, error) {
	cli, err := getClient(e.cfg)
	if err != nil {
		return nil, errors.Wrap(err, "list: failed to get client")
	}
	k := path.Join(e.cfg.KeyPrefix, key)
	nodes, err := list(cli, k)
	if err != nil {
		return nil, errors.Wrap(err, "List: could not get keys")
	}
	var out []string
	for _, f := range filters {
		nodes = filter(nodes, f)
	}
	for _, n := range nodes {
		out = append(out, strings.TrimPrefix(n.Key, e.cfg.KeyPrefix))
	}
	return out, nil
}

// FilterPrefix is a filter to be used with List to return only paths that start with prefix. If specified,
// cut will first trim a leading path off the string before comparison.
func FilterPrefix(prefix string, cut string) func(client.Node) bool {
	return func(n client.Node) bool {
		return strings.HasPrefix(strings.TrimPrefix(n.Key, cut), prefix)
	}
}

// FilterRemoveDirectories is a filter to be used with List to remove all directories (i.e., nodes that contain only other nodes and no value)
func FilterRemoveDirectories() func(client.Node) bool {
	return func(n client.Node) bool {
		return !n.Dir
	}
}

// FilterExactPrefix returns only terminal nodes (files) with the exact path prefix.  For example, for two files
// `/one/two/file.txt` and `/one/two/three/file.txt` only the first would be returned for a prefix of `/one/two`.
func FilterExactPrefix(prefix string, cut string) func(client.Node) bool {
	return func(n client.Node) bool {
		s := strings.TrimPrefix(n.Key, cut)
		if n.Dir {
			return false
		}
		dir, _ := path.Split(s)
		if dir == prefix || dir == prefix+"/" {
			return true
		}
		return false
	}
}

// filter is a helper function to apply filters to the nodes returned from List
func filter(nodes []client.Node, f func(client.Node) bool) []client.Node {
	var out []client.Node
	for _, n := range nodes {
		switch f(n) {
		case true:
			out = append(out, n)
			continue
		default:
			continue
		}
	}
	return out
}

func (e *etcdsrv) prefix() string {
	return e.cfg.KeyPrefix
}

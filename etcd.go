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
}

// Metadata stores information about a particular node that represents a file in etcd
type Metadata struct {
	Key       string
	Size      int
	Timestamp time.Time
	Hash      [20]byte
}

// NewMetadata returns a metadata information given a path and a file to be stored at the path.
// Typically, one metadata node is stored for each file node in etcd.
func NewMetadata(key string, data []byte) *Metadata {
	return &Metadata{
		Key:       key,
		Size:      len(data),
		Timestamp: time.Now().UTC(),
		Hash:      sha1.Sum(data),
	}
}

// Service is a low level interface that stores and loads values in Etcd
type Service interface {
	Store(key string, value []byte) error
	Load(key string) ([]byte, error)
	Metadata(key string) (*Metadata, error)
	Lock() error
	Unlock() error
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
func (e *etcdsrv) Lock() error {
	return e.lock(token)
}

// Lock acquires a lock with a maximum lifetime specified by the ClusterConfig
func (e *etcdsrv) lock(t string) error {
	c, err := getClient(e.cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create etcd client while getting lock")
	}
	acquire := func() error {
		var okToSet bool
		resp, err := c.Get(context.Background(), e.lockKey, nil)
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
			case l.Token == t:
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
				Token:    t,
				Obtained: string(now),
			}
			b, err := json.Marshal(l)
			if err != nil {
				return errors.Wrap(err, "lock: failed to marshal new lock")
			}
			if _, err := c.Set(context.Background(), e.lockKey, base64.StdEncoding.EncodeToString(b), nil); err != nil {
				return errors.Wrap(err, "failed to get lock")
			}
			return nil
		}
		return errors.New("lock: failed to obtain lock, already exists")
	}
	return e.execute(acquire)
}

// Unlock releases the current lock
func (e *etcdsrv) Unlock() error {
	c, err := getClient(e.cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create etcd client while getting lock")
	}
	release := func() error {
		if _, err := c.Delete(context.Background(), e.lockKey, nil); err != nil {
			return errors.Wrap(err, "failed to release lock")
		}
		return nil
	}
	return e.execute(release)
}

func (e *etcdsrv) get(key string, dst *bytes.Buffer) backoff.Operation {
	return func() error {
		cli, err := getClient(e.cfg)
		if err != nil {
			return errors.Wrap(err, "get: could not get client")
		}
		p := path.Join(e.cfg.KeyPrefix, key)
		resp, err := cli.Get(context.Background(), p, nil)
		if err != nil {
			switch {
			case client.IsKeyNotFound(err):
				return nil
			default:
				return errors.Wrap(err, "get: error retrieving value")
			}
		}
		b, err := base64.StdEncoding.DecodeString(resp.Node.Value)
		if err != nil {
			return errors.Wrap(err, "get: error decoding base64 value")
		}
		if _, err := dst.Write(b); err != nil {
			return errors.Wrap(err, "get: error writing node value to destination")
		}
		return nil
	}
}

func (e *etcdsrv) set(key string, value []byte) backoff.Operation {
	return func() error {
		cli, err := getClient(e.cfg)
		if err != nil {
			return errors.Wrap(err, "set: could not get client")
		}
		p := path.Join(e.cfg.KeyPrefix, key)
		if _, err := cli.Set(context.Background(), p, base64.StdEncoding.EncodeToString(value), nil); err != nil {
			return errors.Wrap(err, "set: failed to set key value")
		}
		return nil
	}
}

func (e *etcdsrv) del(key string) backoff.Operation {
	return func() error {
		cli, err := getClient(e.cfg)
		if err != nil {
			return errors.Wrap(err, "del: could not get client")
		}
		p := path.Join(e.cfg.KeyPrefix, key)
		if _, err := cli.Delete(context.Background(), p, nil); err != nil {
			return errors.Wrapf(err, "del: failed to delete key: %s", key)
		}
		return nil
	}
}

func (e *etcdsrv) setMD(m *Metadata) backoff.Operation {
	return func() error {
		cli, err := getClient(e.cfg)
		if err != nil {
			return errors.Wrap(err, "setmd: could not get client")
		}
		jsdata, err := json.Marshal(m)
		key := path.Join(e.mdPrefix, m.Key)
		if _, err := cli.Set(context.Background(), key, base64.StdEncoding.EncodeToString(jsdata), nil); err != nil {
			return errors.Wrap(err, "setmd: failed to set metadata value")
		}
		return nil
	}
}

func (e *etcdsrv) getMD(key string, m *Metadata) backoff.Operation {
	return func() error {
		cli, err := getClient(e.cfg)
		if err != nil {
			return errors.Wrap(err, "getmd: could not get client")
		}
		p := path.Join(e.mdPrefix, key)
		resp, err := cli.Get(context.Background(), p, nil)
		if err != nil {
			return errors.Wrap(err, "getmd: failed to get metadata response")
		}
		bjson, err := base64.StdEncoding.DecodeString(resp.Node.Value)
		if err != nil {
			return errors.Wrap(err, "getmd: failed to decode metadata")
		}
		if err := json.Unmarshal(bjson, m); err != nil {
			return errors.Wrap(err, "getmd: failed to unmarshal metadata response")
		}
		return nil
	}
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

func (e *etcdsrv) Store(key string, value []byte) error {
	return nil
}

func (e *etcdsrv) Load(key string) ([]byte, error) {
	return nil, nil
}

func (e *etcdsrv) Metadata(key string) (*Metadata, error) {
	return nil, nil
}

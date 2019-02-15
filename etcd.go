package etcd

import (
	"bytes"
	"context"
	"crypto/rand"
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

// Metadata stores information about a particular node that represents a file in etcd
type Metadata struct {
	Key       string
	Size      int32
	Timestamp time.Time
}

// EtcdService is a low level interface that stores and loads values in Etcd
type EtcdService interface {
	Store(key string, value []byte) error
	Load(key string) ([]byte, error)
	Metadata(key string) (*Metadata, error)
	Lock() error
	Unlock() error
}

type etcdsrv struct {
	mdPrefix string
	lock     string
	cfg      *ClusterConfig
	client   *client.KeysAPI
}

// NewEtcdService returns a new low level service to store and load values in etcd.  The service is designed to store values with
// associated metadata in a format that allows it to fulfill with the Certmagic storage interface, effectively implementing simple
// filesystem semantics on top of etcd key/value storage.  Locks are acquired before writes to etcd and the library will make its
// best attempt at rolling back transactions that fail.  Concurrent writes are blocking with exponential backoff up to a reasonable
// time limit.  Errors are logged, but do not guarantee that the system will return to a coherent pre-transaction state in the
// presence of significant etcd failures or prolonged unavailability.
func NewEtcdService(c *ClusterConfig) EtcdService {
	return &etcdsrv{
		mdPrefix: path.Join(c.KeyPrefix + "/md"),
		lock:     path.Join(c.KeyPrefix, "/lock"),
		cfg:      c,
	}
}

// Lock acquires a lock with a maximum lifetime specified by the ClusterConfig
func (e *etcdsrv) Lock() error {
	c, err := getClient(e.cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create etcd client while getting lock")
	}
	acquire := func() error {
		if _, err := c.Set(context.Background(), e.lock, token, &client.SetOptions{
			PrevExist: client.PrevNoExist,
			TTL:       e.cfg.LockTimeout,
		}); err != nil {
			return errors.Wrap(err, "failed to get lock")
		}
		return nil
	}
	return backoff.Retry(acquire, backoff.NewExponentialBackOff())
}

// Unlock
func (e *etcdsrv) Unlock() error {
	c, err := getClient(e.cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create etcd client while getting lock")
	}
	release := func() error {
		if _, err := c.Delete(context.Background(), e.lock, nil); err != nil {
			return errors.Wrap(err, "failed to release lock")
		}
		return nil
	}
	return backoff.Retry(release, backoff.NewExponentialBackOff())
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
			return errors.Wrap(err, "store: could not get client")
		}
		p := path.Join(e.cfg.KeyPrefix, key)
		if _, err := cli.Set(context.Background(), p, base64.StdEncoding.EncodeToString(value), nil); err != nil {
			return errors.Wrap(err, "set: failed to set key value")
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

func (e *etcdsrv) Store(key string, value []byte) error {
	return nil
}

func (e *etcdsrv) Load(key string) ([]byte, error) {
	return nil, nil
}

func (e *etcdsrv) Metadata(key string) (*Metadata, error) {
	return nil, nil
}

func tx(txs ...backoff.Operation) []backoff.Operation {
	return txs
}

func pipeline(commits []backoff.Operation, rollbacks []backoff.Operation, b backoff.BackOff) error {
	var err error
	for idx, commit := range commits {
		err = backoff.Retry(commit, b)
		if err != nil {
			for i := idx - 1; i >= 0; i-- {
				switch {
				case i >= len(rollbacks):
					continue
				default:
					if errR := backoff.Retry(rollbacks[i], b); errR != nil {
						err = errors.Wrapf(err, "error on rollback: %s", errR)
					}
				}
			}
			break
		}
	}
	return err
}

func getClient(c *ClusterConfig) (client.KeysAPI, error) {
	cli, err := client.New(client.Config{
		Endpoints: c.ServerIP,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to instantiate etcd client")
	}
	return client.NewKeysAPI(cli), nil
}

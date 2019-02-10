package etcd

import (
	"crypto/rand"
	"github.com/pkg/errors"
	"log"
	"go.etcd.io/etcd/client"
	"time"
	"github.com/cenkalti/backoff"
)

// token is a random value used to manage locks
var token []byte

func init() {
	token = make([]byte, 32)
	_, err := rand.Read(token)
	if err != nil {
		log.Fatal(err)
	}
}

// Metadata stores information about a particular node that represents a file in etcd
type Metadata struct {
	Key string
	Size int32
	Timestamp time.Time
}

// EtcdService is a low level interface that stores and loads values in Etcd
type EtcdService interface {
	Store(key string, value []byte) error
	Load(key string) ([]byte, error)
	Lock() error
	Unlock() error
}

type etcdsrv struct {
	mdPrefix string
	lockPrefix string
	c *ClusterConfig
	client *client.KeysAPI
}

// NewEtcdService returns a new low level service to store and load values in etcd.  The service is designed to store values with
// associated metadata in a format that allows it to fulfill with the Certmagic storage interface, effectively implementing simple
// filesystem semantics on top of etcd key/value storage.  Locks are acquired before writes to etcd and the library will make its
// best attempt at rolling back transactions that fail.  Concurrent writes are blocking with exponential backoff up to a reasonable
// time limit.  Errors are logged, but do not guarantee that the system will return to a coherent pre-transaction state in the
// presence of significant etcd failures or prolonged unavailability.
func NewEtcdService(c *ClusterConfig) EtcdService {
	return &etcdsrv{
		mdPrefix: c.KeyPrefix + "/md",
		lockPrefix: c.KeyPrefix + "/lock",
		c: c,
	}
}

// Lock acquires a lock with a maximum lifetime specified by the ClusterConfig
func (e *etcdsrv) Lock() error {
	return nil
}

// Unlock
func (e *etcdsrv) Unlock() error {
	return nil
}

func (e *etcdsrv) Store(key string, value []byte) error {
	return nil
}

func (e *etcdsrv) Load(key string) ([]byte, error) {
	return nil, nil
}

func tx(txs ...backoff.Operation) []backoff.Operation {
	return txs
}

func pipeline(commits []backoff.Operation, rollbacks []backoff.Operation, b backoff.BackOff)  error {
	var err error
	for idx, commit := range commits {
		err = backoff.Retry(commit, b)
		if err != nil {
			for i := idx-1; i >= 0; i-- {
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
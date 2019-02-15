package etcd

import (
	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/client"
)

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

func tx(txs ...backoff.Operation) []backoff.Operation {
	return txs
}

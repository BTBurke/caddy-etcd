package etcd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"

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

func get(cli client.KeysAPI, key string, dst *bytes.Buffer) backoff.Operation {
	return func() error {
		resp, err := cli.Get(context.Background(), key, nil)
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

func set(cli client.KeysAPI, key string, value []byte) backoff.Operation {
	return func() error {
		if _, err := cli.Set(context.Background(), key, base64.StdEncoding.EncodeToString(value), nil); err != nil {
			return errors.Wrap(err, "set: failed to set key value")
		}
		return nil
	}
}

func del(cli client.KeysAPI, key string) backoff.Operation {
	return func() error {
		if _, err := cli.Delete(context.Background(), key, nil); err != nil {
			return errors.Wrapf(err, "del: failed to delete key: %s", key)
		}
		return nil
	}
}

func setMD(cli client.KeysAPI, key string, m Metadata) backoff.Operation {
	return func() error {
		jsdata, err := json.Marshal(m)
		if err != nil {
			return errors.Wrap(err, "setmd: failed to marshal metadata")
		}
		if _, err := cli.Set(context.Background(), key, base64.StdEncoding.EncodeToString(jsdata), nil); err != nil {
			return errors.Wrap(err, "setmd: failed to set metadata value")
		}
		return nil
	}
}

func getMD(cli client.KeysAPI, key string, m *Metadata) backoff.Operation {
	return func() error {
		resp, err := cli.Get(context.Background(), key, nil)
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

func noop() backoff.Operation {
	return func() error {
		return nil
	}
}

func exists(cli client.KeysAPI, key string, out *bool) backoff.Operation {
	return func() error {
		resp, err := cli.Get(context.Background(), key, nil)
		if err != nil {
			return errors.Wrap(err, "exists: failed to check key")
		}
		switch {
		case resp.Node == nil:
			*out = false
			break
		default:
			*out = true
		}
		return nil
	}
}

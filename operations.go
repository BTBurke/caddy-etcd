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
		resp, err := cli.Get(context.Background(), key, &client.GetOptions{
			Recursive: true,
		})
		if err != nil {
			return errors.Wrap(err, "getmd: failed to get metadata response")
		}
		switch {
		case resp.Node.Dir:
			m.Path = key
			m.IsDir = true
			nodes := new([]client.Node)
			walkNodes(resp.Node, nodes)
			for _, node := range *nodes {
				switch {
				case node.Dir:
					continue
				default:
					md1 := new(Metadata)
					if err := unmarshalMD(&node, md1); err != nil {
						return errors.Wrap(err, "getmd: failed to unmarshal metadata response")
					}
					m.Size = m.Size + md1.Size
					if md1.Timestamp.After(m.Timestamp) {
						m.Timestamp = md1.Timestamp
					}
					continue
				}
			}
			return nil
		default:
			if err := unmarshalMD(resp.Node, m); err != nil {
				return errors.Wrap(err, "getmd: failed to unmarshal metadata response")
			}
			return nil
		}
	}
}

func unmarshalMD(node *client.Node, m *Metadata) error {
	if node == nil || m == nil {
		return errors.New("unmarshalMD: response or metadata is nil")
	}
	bjson, err := base64.StdEncoding.DecodeString(node.Value)
	if err != nil {
		return errors.Wrap(err, "getmd: failed to decode metadata")
	}
	if err := json.Unmarshal(bjson, m); err != nil {
		return errors.Wrap(err, "getmd: failed to unmarshal metadata response")
	}
	return nil
}

func noop() backoff.Operation {
	return func() error {
		return nil
	}
}

func exists(cli client.KeysAPI, key string, out *bool) backoff.Operation {
	return func() error {
		_, err := cli.Get(context.Background(), key, nil)
		if err != nil {
			switch {
			case client.IsKeyNotFound(err):
				*out = false
				return nil
			default:
				return errors.Wrap(err, "exists: failed to check key")
			}
		}
		*out = true
		return nil
	}
}

func list(cli client.KeysAPI, key string) ([]client.Node, error) {

	var out []client.Node
	resp := new(client.Response)
	getRecursive := func() error {
		var err error
		resp, err = cli.Get(context.Background(), key, &client.GetOptions{
			Recursive: true,
		})
		if err != nil {
			switch {
			case client.IsKeyNotFound(err):
				return nil
			default:
				return errors.Wrap(err, "list: unable to get list")
			}
		}
		return nil
	}
	if err := backoff.Retry(getRecursive, backoff.NewExponentialBackOff()); err != nil {
		return nil, err
	}
	if resp == nil {
		return out, nil
	}
	walkNodes(resp.Node, &out)
	return out, nil

}

func walkNodes(node *client.Node, out *[]client.Node) {
	*out = append(*out, *node)
	for _, n := range node.Nodes {
		switch {
		case n.Nodes == nil:
			*out = append(*out, *n)
			continue
		default:
			walkNodes(n, out)
			continue
		}
	}
}

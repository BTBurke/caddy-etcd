package etcd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/client"
)

func shouldRunIntegration() bool {
	resp, err := http.Get("http://127.0.0.1:2379/version")
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func TestPipeline(t *testing.T) {
	var arr []int
	push := func(n int, shouldErr bool) backoff.Operation {
		return func() error {
			if shouldErr {
				return errors.New("push error")
			}
			arr = append(arr, n)
			return nil
		}
	}
	pop := func() backoff.Operation {
		return func() error {
			arr = arr[0 : len(arr)-1]
			return nil
		}
	}
	noop := func() backoff.Operation {
		return func() error {
			return nil
		}
	}

	var tcs = []struct {
		Commit    []backoff.Operation
		Rollback  []backoff.Operation
		ShouldErr bool
		Expect    []int
	}{
		{Commit: tx(push(1, false), push(2, false)), Rollback: nil, ShouldErr: false, Expect: []int{1, 2}},
		{Commit: tx(push(1, false), push(2, true)), Rollback: tx(pop(), pop()), ShouldErr: true, Expect: []int{}},
		{Commit: tx(push(1, false), push(2, true)), Rollback: tx(pop()), ShouldErr: true, Expect: []int{}},
		{Commit: tx(push(1, false), push(2, true)), Rollback: nil, ShouldErr: true, Expect: []int{1}},
		{Commit: tx(push(1, false), push(2, false), push(3, true)), Rollback: tx(pop(), noop(), pop()), ShouldErr: true, Expect: []int{1}},
	}
	for _, tc := range tcs {
		arr = []int{}
		err := pipeline(tc.Commit, tc.Rollback, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 1))
		switch tc.ShouldErr {
		case true:
			assert.Error(t, err)
		default:
			assert.NoError(t, err)
		}
		assert.Equal(t, tc.Expect, arr)
	}
}

func TestLowLevelSet(t *testing.T) {
	if !shouldRunIntegration() {
		t.Skip("no etcd server found, skipping")
	}
	cfg := &ClusterConfig{
		KeyPrefix: "/caddy",
		ServerIP:  []string{"http://127.0.0.1:2379"},
	}
	cli := &etcdsrv{
		mdPrefix: path.Join(cfg.KeyPrefix + "/md"),
		lockKey:  path.Join(cfg.KeyPrefix, "/lock"),
		cfg:      cfg,
	}
	tcs := []struct {
		Path  string
		Value []byte
	}{
		{Path: "test", Value: []byte("test")},
		{Path: "/test", Value: []byte("test")},
		{Path: "/deeply/nested/value", Value: []byte("test")},
	}
	for _, tc := range tcs {
		err := cli.set(tc.Path, tc.Value)()
		assert.NoError(t, err)
		resp, err := http.Get("http://127.0.0.1:2379/v2/keys/caddy/" + tc.Path)
		if err != nil {
			t.Fail()
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fail()
		}
		var node client.Response
		if err := json.Unmarshal(body, &node); err != nil {
			t.Fail()
		}
		//t.Logf("resp: key: %v value: %v\n", node.Node.Key, node.Node.Value)
		assert.Equal(t, path.Join("/caddy", tc.Path), node.Node.Key)
		assert.Equal(t, base64.StdEncoding.EncodeToString(tc.Value), node.Node.Value)
	}
}

func TestLowLevelGet(t *testing.T) {
	if !shouldRunIntegration() {
		t.Skip("no etcd server found, skipping")
	}
	cfg := &ClusterConfig{
		KeyPrefix: "/caddy",
		ServerIP:  []string{"http://127.0.0.1:2379"},
	}
	cli := &etcdsrv{
		mdPrefix: path.Join(cfg.KeyPrefix + "/md"),
		lockKey:  path.Join(cfg.KeyPrefix, "/lock"),
		cfg:      cfg,
	}
	tcs := []struct {
		Path  string
		Value []byte
	}{
		{Path: "test", Value: []byte("test")},
		{Path: "/test", Value: []byte("test")},
		{Path: "/deeply/nested/value", Value: []byte("test")},
	}
	for _, tc := range tcs {
		if err := cli.set(tc.Path, tc.Value)(); err != nil {
			t.Fail()
		}
		var buf bytes.Buffer
		err := cli.get(tc.Path, &buf)()
		resp, err := ioutil.ReadAll(&buf)
		if err != nil {
			t.Fail()
		}
		assert.NoError(t, err)
		assert.Equal(t, tc.Value, resp)
	}
}

func TestLockUnlock(t *testing.T) {
	if !shouldRunIntegration() {
		t.Skip("no etcd server found, skipping")
	}
	token = "testtoken"
	cfg := &ClusterConfig{
		KeyPrefix: "/caddy",
		ServerIP:  []string{"http://127.0.0.1:2379"},
	}
	cli := &etcdsrv{
		mdPrefix:  path.Join(cfg.KeyPrefix + "/md"),
		lockKey:   path.Join(cfg.KeyPrefix, "/lock"),
		cfg:       cfg,
		noBackoff: true,
	}
	type lockFunc func(d time.Duration) error
	lock := func(t string) lockFunc {
		return func(d time.Duration) error {
			log.Printf("getting lock with timeout %s\n", d)
			cli.cfg.LockTimeout = d
			return cli.lock(t)
		}
	}
	unlock := func(d time.Duration) error {
		cli.cfg.LockTimeout = d
		return cli.Unlock()
	}
	wait := func(d time.Duration) lockFunc {
		return func(d2 time.Duration) error {
			log.Printf("waiting %s, start: %s\n", d, time.Now())
			time.Sleep(d)
			log.Printf("end sleep: %s", time.Now())
			return nil
		}
	}

	tcs := []struct {
		Name      string
		Timeout   time.Duration
		Funcs     []lockFunc
		ShouldErr bool
	}{
		{Name: "Lock Unlock", Timeout: 5 * time.Second, Funcs: []lockFunc{lock("test"), unlock}, ShouldErr: false},
		{Name: "Lock while locked different clients", Timeout: 5 * time.Second, Funcs: []lockFunc{lock("test"), lock("test2")}, ShouldErr: true},
		{Name: "Lock after timeout", Timeout: 1 * time.Second, Funcs: []lockFunc{lock("test"), wait(2 * time.Second), lock("test")}, ShouldErr: false},
		{Name: "Lock while locked extend lock", Timeout: 5 * time.Second, Funcs: []lockFunc{lock("test"), lock("test")}, ShouldErr: false},
	}
	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			if err := cli.del("/lock"); err != nil {
				t.Log(err)
			}
			var err error
			for _, f := range tc.Funcs {
				err = f(tc.Timeout)
			}
			switch tc.ShouldErr {
			case true:
				assert.Error(t, err)
			default:
				assert.NoError(t, err)
			}
		})
	}
}

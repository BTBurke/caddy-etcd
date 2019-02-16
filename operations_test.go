package etcd

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"strings"
	"testing"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/client"
)

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
	tcs := []struct {
		Path  string
		Value []byte
	}{
		{Path: "test", Value: []byte("test")},
		{Path: "/test", Value: []byte("test")},
		{Path: "/deeply/nested/value.md", Value: []byte("test")},
	}
	for _, tc := range tcs {
		cli, err := getClient(cfg)
		assert.NoError(t, err)
		errC := set(cli, path.Join(cfg.KeyPrefix, tc.Path), tc.Value)()
		assert.NoError(t, errC)
		resp, err := http.Get("http://127.0.0.1:2379" + path.Join("/v2/keys/caddy/", tc.Path))
		if err != nil {
			log.Print(err)
			t.FailNow()
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
	tcs := []struct {
		Path  string
		Value []byte
	}{
		{Path: "test", Value: []byte("test")},
		{Path: "/test", Value: []byte("test")},
		{Path: "/deeply/nested/value", Value: []byte("test")},
	}
	for _, tc := range tcs {
		cli, err := getClient(cfg)
		if err != nil {
			t.Fail()
		}
		if err := set(cli, cfg.KeyPrefix+tc.Path, tc.Value)(); err != nil {
			t.Fail()
		}
		var buf bytes.Buffer
		errC := get(cli, cfg.KeyPrefix+tc.Path, &buf)()
		resp, err := ioutil.ReadAll(&buf)
		if err != nil {
			t.Fail()
		}
		assert.NoError(t, errC)
		assert.Equal(t, tc.Value, resp)
	}
}

func TestLowLevelMD(t *testing.T) {

	if !shouldRunIntegration() {
		t.Skip("no etcd server found, skipping")
	}
	cfg := &ClusterConfig{
		KeyPrefix: "/caddy",
		ServerIP:  []string{"http://127.0.0.1:2379"},
	}
	data := []byte("test data")
	expSum := sha1.Sum(data)
	p := "/some/path/key.md"
	key := path.Join(cfg.KeyPrefix, p)
	md := NewMetadata(p, data)
	assert.Equal(t, p, md.Path)
	assert.Equal(t, expSum, md.Hash)
	assert.Equal(t, len(data), md.Size)
	cli, err := getClient(cfg)
	if err != nil {
		t.Fail()
	}
	if err := setMD(cli, key, md)(); err != nil {
		assert.NoError(t, err)
	}
	var md2 Metadata
	if err := getMD(cli, key, &md2)(); err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, md, md2)
	assert.Equal(t, expSum, md2.Hash)
	assert.Equal(t, len(data), md2.Size)
	assert.Equal(t, p, md2.Path)
}

func TestListLowLevel(t *testing.T) {
	if !shouldRunIntegration() {
		t.Skip("no etcd server found, skipping")
	}
	cfg := &ClusterConfig{
		KeyPrefix: "/caddy",
		ServerIP:  []string{"http://127.0.0.1:2379"},
	}
	paths := []string{
		"/one/two/three.end",
		"/one/two/four.end",
		"/one/five/six/seven.end",
		"/one/five/eleven.end",
		"/one/five/six/ten.end",
	}
	cli, err := getClient(cfg)
	assert.NoError(t, err)
	for _, p := range paths {
		if err := set(cli, path.Join(cfg.KeyPrefix, p), []byte("test"))(); err != nil {
			assert.NoError(t, err)
		}
	}
	out, err := list(cli, path.Join(cfg.KeyPrefix, "one"))
	assert.NoError(t, err)
	var s []string
	for _, n := range out {
		s = append(s, strings.TrimPrefix(n.Key, cfg.KeyPrefix))
	}
	for _, p := range paths {
		assert.Contains(t, s, p)
	}
}

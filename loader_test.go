package etcd

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	if !shouldRunIntegration() {
		t.Skip("no etcd server found, skipping")
	}
	cf1 := []byte("cf1.cluster.local {\n\tproxy test:123\n}")
	cf2 := []byte("cf2.cluster.local {\n\tproxy test:123\n}")
	var f *os.File

	cfg := &ClusterConfig{
		KeyPrefix: "/caddy",
		ServerIP:  []string{"http://127.0.0.1:2379"},
	}
	cliL, err := getClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	type testFunc func() error
	setCF := func(val []byte) testFunc {
		return func() error {
			return set(cliL, path.Join(cfg.KeyPrefix, "caddyfile"), val)()
		}
	}
	reset := func() error {
		if err := os.Unsetenv("CADDY_CLUSTERING_ETCD_CADDYFILE"); err != nil {
			return err
		}
		del(cliL, path.Join(cfg.KeyPrefix, "caddyfile"))()
		return nil
	}
	createCF := func(val []byte) testFunc {
		return func() error {
			var err error
			f, err = ioutil.TempFile("", "caddyfile")
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := f.Write(val); err != nil {
				return err
			}
			if err := os.Setenv("CADDY_CLUSTERING_ETCD_CADDYFILE", f.Name()); err != nil {
				return err
			}
			return nil
		}
	}
	disableLoad := func() error {
		return os.Setenv("CADDY_CLUSTERING_ETCD_CADDYFILE_LOADER", "disable")
	}
	tcs := []struct {
		Name   string
		Funcs  []testFunc
		Expect []byte
	}{
		{Name: "from etcd", Funcs: []testFunc{setCF(cf1)}, Expect: cf1},
		{Name: "from file", Funcs: []testFunc{createCF(cf1)}, Expect: cf1},
		{Name: "prefer etcd over file", Funcs: []testFunc{setCF(cf1), createCF(cf2)}, Expect: cf1},
		{Name: "disable", Funcs: []testFunc{disableLoad}, Expect: nil},
	}
	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			if err := reset(); err != nil {
				t.Fatal(err)
			}
			for _, f := range tc.Funcs {
				if err := f(); err != nil {
					t.Fatal(err)
				}
			}

			l, err := Load("http")
			assert.NoError(t, err)
			switch tc.Expect {
			case nil:
				assert.Nil(t, l)
			default:
				assert.Equal(t, tc.Expect, l.Body())
			}

			// check etcd persists caddyfile
			var actualEtcd bytes.Buffer
			if err := get(cliL, path.Join(cfg.KeyPrefix, "caddyfile"), &actualEtcd)(); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.Expect, actualEtcd.Bytes())
		})

	}
}

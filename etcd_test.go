package etcd

import (
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func shouldRunIntegration() bool {
	resp, err := http.Get("http://127.0.0.1:2379/version")
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	return true
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
			time.Sleep(d)
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
			cliL, errL := getClient(cfg)
			if errL != nil {
				t.Fail()
			}
			_ = del(cliL, cfg.KeyPrefix+"/lock")
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

func TestMetadata(t *testing.T) {
	if !shouldRunIntegration() {
		t.Skip("no etcd server found, skipping")
	}
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
	p := "/some/path/key.md"
	data1 := []byte("test data")
	md1 := NewMetadata(p, data1)
	cliL, err := getClient(cfg)
	assert.NoError(t, err)
	if err := cli.execute(setMD(cliL, path.Join(cli.mdPrefix, p), md1)); err != nil {
		assert.NoError(t, err)
	}
	md, err := cli.Metadata(p)
	assert.NoError(t, err)
	assert.Equal(t, md1, *md)
	md2, err2 := cli.Metadata("/does/not/exist")
	assert.Error(t, err2)
	assert.Nil(t, md2)
	assert.True(t, IsNotExistError(err2))
}

func TestStoreLoad(t *testing.T) {
	if !shouldRunIntegration() {
		t.Skip("no etcd server found, skipping")
	}
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
	p := "/some/path/key.md"
	data1 := []byte("test data")
	data2 := []byte("test data 2")
	md1 := NewMetadata(p, data1)
	md2 := NewMetadata(p, data2)
	if err := cli.Store(p, data1); err != nil {
		assert.NoError(t, err)
	}
	md1R, err := cli.Metadata(p)
	assert.NoError(t, err)
	assert.Equal(t, md1.Path, md1R.Path)
	assert.Equal(t, md1.Hash, md1R.Hash)
	assert.Equal(t, md1.Size, md1R.Size)
	data1R, err := cli.Load(p)
	assert.NoError(t, err)
	assert.Equal(t, data1, data1R)
	if err := cli.Store(p, data2); err != nil {
		assert.NoError(t, err)
	}
	md2R, err := cli.Metadata(p)
	assert.NoError(t, err)
	assert.Equal(t, md2.Path, md2R.Path)
	assert.Equal(t, md2.Hash, md2R.Hash)
	assert.Equal(t, md2.Size, md2R.Size)
	data2R, err := cli.Load(p)
	assert.Equal(t, data2, data2R)
	assert.NoError(t, err)

}

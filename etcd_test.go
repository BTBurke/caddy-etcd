package etcd

import (
	"net/http"
	"path"
	"strings"
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
	lock := func(t string, key string) lockFunc {
		return func(d time.Duration) error {
			cli.cfg.LockTimeout = d
			return cli.lock(t, key)
		}
	}
	unlock := func(key string) lockFunc {
		return func(d time.Duration) error {
			cli.cfg.LockTimeout = d
			return cli.Unlock(key)
		}
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
		{Name: "Lock Unlock", Timeout: 5 * time.Second, Funcs: []lockFunc{lock("test", "/path/one.md"), unlock("/path/one.md")}, ShouldErr: false},
		{Name: "Lock while locked different clients", Timeout: 5 * time.Second, Funcs: []lockFunc{lock("test", "/path/one.md"), lock("test2", "/path/one.md")}, ShouldErr: true},
		{Name: "Lock after timeout", Timeout: 1 * time.Second, Funcs: []lockFunc{lock("test", "/path/one.md"), wait(2 * time.Second), lock("test", "/path/one.md")}, ShouldErr: false},
		{Name: "Lock while locked extend lock", Timeout: 5 * time.Second, Funcs: []lockFunc{lock("test", "/path/one.md"), lock("test", "/path/one.md")}, ShouldErr: false},
		{Name: "Locks on different paths", Timeout: 5 * time.Second, Funcs: []lockFunc{lock("test", "/path/one.md"), lock("test", "/path/two.md")}, ShouldErr: false},
	}
	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			cliL, errL := getClient(cfg)
			if errL != nil {
				t.Fail()
			}
			_ = del(cliL, cfg.KeyPrefix+"/lock/path/one.md")
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

func TestList(t *testing.T) {
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
		"/one/two/three/four.end",
		"/one/five/six/seven.end",
		"/one/five/eleven.end",
		"/one/five/six/ten.end",
	}
	cliL, err := getClient(cfg)
	assert.NoError(t, err)
	for _, p := range paths {
		if err := set(cliL, path.Join(cfg.KeyPrefix, p), []byte("test"))(); err != nil {
			assert.NoError(t, err)
		}
	}
	cli := &etcdsrv{
		mdPrefix:  path.Join(cfg.KeyPrefix + "/md"),
		lockKey:   path.Join(cfg.KeyPrefix, "/lock"),
		cfg:       cfg,
		noBackoff: true,
	}
	out1, err := cli.List("/one")
	assert.NoError(t, err)
	for _, p := range paths {
		assert.Contains(t, out1, p)
	}
	out2, err := cli.List("/one", FilterPrefix("/one/two", cfg.KeyPrefix))
	assert.NoError(t, err)
	for _, p := range paths {
		if strings.HasPrefix(p, "/one/two") {
			assert.Contains(t, out2, p)
		} else {
			assert.NotContains(t, out2, p)
		}
	}
	out3, err := cli.List("/one", FilterRemoveDirectories())
	assert.NoError(t, err)
	for _, p := range paths {
		dir, _ := path.Split(p)
		assert.NotContains(t, out3, dir)
		assert.Contains(t, out3, p)
	}
	out4, err := cli.List("/one", FilterExactPrefix("/one/two", cfg.KeyPrefix))
	assert.NoError(t, err)
	assert.Contains(t, out4, "/one/two/three.end")
	assert.Contains(t, out4, "/one/two/four.end")
	assert.NotContains(t, out4, "/one/two/three/four.end")
	out5, err := cli.List("/one/two")
	assert.Contains(t, out5, "/one/two/three.end")
	assert.Contains(t, out5, "/one/two/four.end")
	assert.Contains(t, out5, "/one/two/three/four.end")
	assert.NotContains(t, out5, "/one/five/eleven.md")
	out6, err := cli.List("/one/two", FilterPrefix("/one/five", cfg.KeyPrefix))
	assert.NoError(t, err)
	assert.Empty(t, out6)
}

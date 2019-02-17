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
	data := []byte("test data")
	//dataSize := len(data)
	paths := map[string]Metadata{
		"/testmd/some/path/key1.md":        NewMetadata("/testmd/some/path/key1.md", data),
		"/testmd/some/path/key2.md":        NewMetadata("/testmd/some/path/key2.md", data),
		"/testmd/some/path/deeper/key3.md": NewMetadata("/testmd/some/path/deeper/key3.md", data),
	}
	lastTime := func(p map[string]Metadata) time.Time {
		var t1 time.Time
		for _, md := range p {
			if md.Timestamp.After(t1) {
				t1 = md.Timestamp
			}
		}
		return t1
	}
	tcs := []struct {
		Name        string
		Path        string
		Expect      Metadata
		ShouldExist bool
	}{
		{Name: "basic get", Path: "/testmd/some/path/key1.md", Expect: paths["/testmd/some/path/key1.md"], ShouldExist: true},
		{Name: "basic get nested", Path: "/testmd/some/path/deeper/key3.md", Expect: paths["/testmd/some/path/deeper/key3.md"], ShouldExist: true},
		{Name: "not exist", Path: "/does/not/exist", Expect: Metadata{}, ShouldExist: false},
		{Name: "nested directory", Path: "/testmd/some/path", Expect: Metadata{Path: "/testmd/some/path", Size: 3 * len(data), IsDir: true, Timestamp: lastTime(paths)}, ShouldExist: true},
	}
	cliL, err := getClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range paths {
		if err := cli.execute(setMD(cliL, path.Join(cli.mdPrefix, k), v)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			md, err := cli.Metadata(tc.Path)
			switch {
			case tc.ShouldExist:
				assert.Equal(t, tc.Expect, *md)
				assert.NoError(t, err)
			default:
				assert.Error(t, err)
				assert.True(t, IsNotExistError(err))
			}

		})
	}
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
	p := "/path/key.md"
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

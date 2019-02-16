package etcd

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPrefix(t *testing.T) {
	tcs := []struct {
		Path   string
		Expect string
	}{
		{Path: "/caddy/", Expect: "/caddy"},
		{Path: "\\caddy", Expect: "/caddy"},
		{Path: "\\caddy", Expect: "/caddy"},
		{Path: "//caddy", Expect: "/caddy"},
		{Path: "//caddy//test", Expect: "/caddy/test"},
		{Path: "caddy", Expect: "/caddy"},
		{Path: "caddy//test", Expect: "/caddy/test"},
	}
	for _, tc := range tcs {
		c, err := NewClusterConfig(WithPrefix(tc.Path))
		assert.NoError(t, err)
		assert.Equal(t, tc.Expect, c.KeyPrefix)
	}
}

func TestServers(t *testing.T) {
	tcs := []struct {
		Name      string
		SString   string
		Expect    []string
		ShouldErr bool
	}{
		{Name: "1", SString: "http://127.0.0.1:2379", Expect: []string{"http://127.0.0.1:2379"}, ShouldErr: false},
		{Name: "2 comma", SString: "http://127.0.0.1:2379,http://127.0.0.1:2380", Expect: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2380"}, ShouldErr: false},
		{Name: "2 comma ws", SString: "http://127.0.0.1:2379, http://127.0.0.1:2380", Expect: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2380"}, ShouldErr: false},
		{Name: "2 comma ws2", SString: "http://127.0.0.1:2379 , http://127.0.0.1:2380", Expect: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2380"}, ShouldErr: false},
		{Name: "2 semicolon", SString: "http://127.0.0.1:2379;http://127.0.0.1:2380", Expect: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2380"}, ShouldErr: false},
		{Name: "no scheme", SString: "127.0.0.1:2379", Expect: []string{}, ShouldErr: true},
		{Name: "https", SString: "https://127.0.0.1:2379", Expect: []string{"https://127.0.0.1:2379"}, ShouldErr: false},
		{Name: "no scheme dns", SString: "etcd", Expect: []string{}, ShouldErr: true},
		{Name: "scheme dns", SString: "http://etcd", Expect: []string{"http://etcd"}, ShouldErr: false},
	}
	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			c, err := NewClusterConfig(WithServers(tc.SString))
			switch {
			case tc.ShouldErr:
				assert.Error(t, err)
				assert.Nil(t, c)
			default:
				assert.NoError(t, err)
				assert.Equal(t, tc.Expect, c.ServerIP)
			}
		})
	}
}

func TestCaddyfile(t *testing.T) {
	caddyfile := []byte("example.com {\n\tproxy http://127.0.0.1:8080\n}")
	f, err := ioutil.TempFile("", "Caddyfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	if _, err := f.Write(caddyfile); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	c, err := NewClusterConfig(WithCaddyFile(f.Name()))
	assert.NoError(t, err)
	assert.Equal(t, caddyfile, c.CaddyFile)
}

func TestTimeout(t *testing.T) {
	tcs := []struct {
		Name      string
		Input     string
		Expected  time.Duration
		ShouldErr bool
	}{
		{Name: "ok", Input: "5m", Expected: time.Minute * 5, ShouldErr: false},
		{Name: "ok small", Input: "2s", Expected: time.Second * 2, ShouldErr: false},
		{Name: "not ok", Input: "2y", Expected: time.Second, ShouldErr: true},
	}
	for _, tc := range tcs {
		c, err := NewClusterConfig(WithTimeout(tc.Input))
		switch {
		case tc.ShouldErr:
			assert.Nil(t, c)
			assert.Error(t, err)
		default:
			assert.NoError(t, err)
			assert.Equal(t, tc.Expected, c.LockTimeout)
		}
	}
}

func TestConfigOpts(t *testing.T) {
	caddyfile := []byte("example.com {\n\tproxy http://127.0.0.1:8080\n}")
	f, err := ioutil.TempFile("", "Caddyfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	if _, err := f.Write(caddyfile); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	set := func(e map[string]string) error {
		for k, v := range e {
			if err := os.Setenv(k, v); err != nil {
				return err
			}
		}
		return nil
	}
	unset := func(e map[string]string) error {
		for k := range e {
			if err := os.Unsetenv(k); err != nil {
				return err
			}
		}
		return nil
	}
	env := map[string]string{
		"CADDY_CLUSTERING_ETCD_SERVERS":   "http://127.0.0.1:2379",
		"CADDY_CLUSTERING_ETCD_PREFIX":    "/test",
		"CADDY_CLUSTERING_ETCD_TIMEOUT":   "30m",
		"CADDY_CLUSTERING_ETCD_CADDYFILE": f.Name(),
	}
	env2 := map[string]string{
		"CADDY_CLUSTERING_ETCD_SERVERS":   "http://127.0.0.1:2379",
		"CADDY_CLUSTERING_ETCD_PREFIX":    "/test",
		"CADDY_CLUSTERING_ETCD_TIMEOUT":   "30y",
		"CADDY_CLUSTERING_ETCD_CADDYFILE": f.Name(),
	}
	tcs := []struct {
		Name      string
		Input     map[string]string
		Expect    ClusterConfig
		ShouldErr bool
	}{
		{Name: "ok", Input: env, Expect: ClusterConfig{ServerIP: []string{"http://127.0.0.1:2379"}, LockTimeout: 30 * time.Minute, KeyPrefix: "/test", CaddyFile: caddyfile}, ShouldErr: false},
		{Name: "should err", Input: env2, Expect: ClusterConfig{}, ShouldErr: true},
	}
	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			if err := unset(tc.Input); err != nil {
				t.Fatal(err)
			}
			if err := set(tc.Input); err != nil {
				t.Fatal(err)
			}
			opts := ConfigOptsFromEnvironment()
			c, err := NewClusterConfig(opts...)
			switch {
			case tc.ShouldErr:
				assert.Error(t, err)
				assert.Nil(t, c)
			default:
				assert.NoError(t, err)
				assert.Equal(t, tc.Expect, *c)
			}
			_ = unset(tc.Input)
		})
	}

}

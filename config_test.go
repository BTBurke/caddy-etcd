package etcd

import (
	"testing"

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

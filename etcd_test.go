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

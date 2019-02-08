package etcd

import (
	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"testing"
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

	var tcs = []struct{
		Commit []backoff.Operation
		Rollback []backoff.Operation
		ShouldErr bool
		Expect []int
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
		switch tc.ShouldErr{
		case true:
			assert.Error(t, err)
		default:
			assert.NoError(t, err)
		}
		assert.Equal(t, tc.Expect, arr)
	}
}
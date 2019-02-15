package etcd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {
	e1 := NotExist{"/test/path"}
	e2 := FailedChecksum{"/test/path"}
	assert.True(t, IsNotExistError(e1))
	assert.True(t, IsFailedChecksumError(e2))
}

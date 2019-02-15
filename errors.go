package etcd

import "fmt"

type NotExist struct {
	Key string
}

func (e NotExist) Error() string {
	return fmt.Sprintf("key %s does not exist", e.Key)
}

func IsNotExistError(e error) bool {
	switch e.(type) {
	case NotExist:
		return true
	default:
		return false
	}
}

type FailedChecksum struct {
	Key string
}

func (e FailedChecksum) Error() string {
	return fmt.Sprintf("key %s does not exist", e.Key)
}

func IsFailedChecksumError(e error) bool {
	switch e.(type) {
	case FailedChecksum:
		return true
	default:
		return false
	}
}

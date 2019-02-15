package etcd

import "fmt"

// NotExist is returned when a key lookup fails when calling Load or Metadata
type NotExist struct {
	Key string
}

func (e NotExist) Error() string {
	return fmt.Sprintf("key %s does not exist", e.Key)
}

// IsNotExistError checks to see if error is of type NotExist
func IsNotExistError(e error) bool {
	switch e.(type) {
	case NotExist:
		return true
	default:
		return false
	}
}

// FailedChecksum error is returned when the data retured by Load does not match the
// SHA1 checksum stored in its metadata node
type FailedChecksum struct {
	Key string
}

func (e FailedChecksum) Error() string {
	return fmt.Sprintf("key %s does not exist", e.Key)
}

// IsFailedChecksumError checks to see if error is of type FailedChecksum
func IsFailedChecksumError(e error) bool {
	switch e.(type) {
	case FailedChecksum:
		return true
	default:
		return false
	}
}

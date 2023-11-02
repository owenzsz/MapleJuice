package global

import (
	"fmt"
	"time"
)

const (
	LOGGER_PORT          = "55555"
	FD_PORT              = "55556"
	SDFS_PORT            = "55557"
	LEADER_ELECTION_PORT = "55558"

	QUORUM                = 6
	RETRY_CONN_SLEEP_TIME = 3 * time.Second
	RETRY_LOCK_SLEEP_TIME = 1 * time.Second
)

var (
	// This is supposed to be const, DO NOT MODIFY!!
	SERVER_ADDRS = []string{
		"fa23-cs425-1801.cs.illinois.edu", "fa23-cs425-1802.cs.illinois.edu",
		"fa23-cs425-1803.cs.illinois.edu", "fa23-cs425-1804.cs.illinois.edu",
		"fa23-cs425-1805.cs.illinois.edu", "fa23-cs425-1806.cs.illinois.edu",
		"fa23-cs425-1807.cs.illinois.edu", "fa23-cs425-1808.cs.illinois.edu",
		"fa23-cs425-1809.cs.illinois.edu", "fa23-cs425-1810.cs.illinois.edu"}
)

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// adapted from: https://stackoverflow.com/questions/10485743/contains-method-for-a-slice
func Contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}

func RemoveElementFromFirstTwo[T comparable](s []T, e T) ([]T, error) {
	var idx int
	var found bool
	for i, v := range s {
		if i >= 2 {
			break
		}
		if v == e {
			idx = i
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("element not found in first two elements of slice")
	}
	return append(s[:idx], s[idx+1:]...), nil
}

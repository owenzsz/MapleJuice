package global

import (
	"time"
)

const (
	LOGGER_PORT                   = "55555"
	FD_PORT                       = "55556"
	SDFS_PORT                     = "55557"
	LEADER_ELECTION_PORT          = "55558"
	MAPLE_JUICE_PORT              = "55559"
	LEADER_STATE_REPLICATION_PORT = "55560"

	QUORUM                = 5
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

var MP_NUMBER int

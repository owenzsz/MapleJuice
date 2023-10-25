package global

const (
	LOGGER_PORT = "55559"
	FD_PORT     = "55560"
	SDFS_PORT   = "55561"
	LEADER_ELECTION_PORT = "55562"

	QUORUM = 6
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

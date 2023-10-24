package global

const (
	LOGGER_PORT = "55555"
	FD_PORT     = "55556"
	SDFS_PORT   = "55557"
)

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

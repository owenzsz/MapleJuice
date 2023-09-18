package failureDetector

import (
	"fmt"
	"sync"
	"time"
)

const (
	GOSSIP_RATE         = 1000 * time.Millisecond // 1000ms
	T_FAIL              = 2                       // 2 seconds
	T_CLEANUP           = 3                       // 3 seconds
	NUM_NODES_TO_GOSSIP = 3                       //number of nodes to gossip to
	PORT                = "55556"
	INTRODUCER_PORT     = "55557"
	HOST                = "0.0.0.0"
)

type MessageType int
type StatusType int

const (
	Alive StatusType = iota
	Suspected
	Failed
)

const (
	Join MessageType = iota
	Leave
	Gossip
)

var (
	nodeList           = make(map[string]*Node)
	USE_SUSPICION      = false
	MESSAGE_DROP_RATE  = 0.0
	LOCAL_NODE_KEY     = getLocalNodeName() + fmt.Sprint(time.Now().Unix())
	nodeListLock       = &sync.Mutex{}
	INTRODUCER_ADDRESS = "fa23-cs425-1801.cs.illinois.edu"
)

type Node struct {
	Address          string     `json:"Address"`
	HeartbeatCounter int        `json:"heartbeatCounter"`
	Status           StatusType `json:"status"`
	TimeStamp        int        `json:"timeStamp"`
}

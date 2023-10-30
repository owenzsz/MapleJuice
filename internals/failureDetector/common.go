package failureDetector

import (
	pb "cs425-mp/protobuf"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
	"cs425-mp/internals/global"
)

const (
	INTRODUCER_ADDRESS = "fa23-cs425-1801.cs.illinois.edu" // Introducer node's receiving address
	CONN_TIMEOUT       = 500 * time.Millisecond
)

var (
	GOSSIP_RATE         = 400 * time.Millisecond // 400 ms
	T_FAIL              = 4 * time.Second        // 4 seconds
	T_SUSPECT           = 2 * time.Second        // 2 seconds
	T_CLEANUP           = 3 * time.Second        // 3 seconds
	NUM_NODES_TO_GOSSIP = 2                      //number of nodes to gossip to

	NodeInfoList      = make(map[string]*Node) // Membership list
	USE_SUSPICION     = false                  // whether to use suspicion mode. Default is off. Can be changed through http endpoint
	MESSAGE_DROP_RATE = 0.0                    // drop rate. Default is 0%. Can be changed through http endpoint
	LOCAL_NODE_KEY    = ""                     // current node's ID key. Dynamically generated every time a node is brought up or rejoined
	NodeListLock      = &sync.Mutex{}          // mutex to protect internal states

	DNS_Cache_Lock = &sync.Mutex{}           // mutex to protect DNS_Cache
	DNS_Cache      = make(map[string]string) // local cache to store the response of DNS request
	SDFS_CHANNEL   chan string               // channel to communicate with SDFS
)

// Node struct to represent each row in the membership list
type Node struct {
	NodeAddr  string
	SeqNo     int32
	Status    StatusType
	TimeStamp time.Time
}

type MessageType int
type StatusType int

// Node status types
const (
	Alive StatusType = iota
	Suspected
	Failed
	Left
)

// stringify helper for StatusType
func (e StatusType) String() string {
	switch e {
	case Alive:
		return "Alive"
	case Suspected:
		return "Suspected"
	case Failed:
		return "Failed"
	default:
		return "Unknown status type. Please check StatusType enum"
	}
}

// visualization helper for StatusType
func (s StatusType) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// Node message types
const (
	Join MessageType = iota
	Leave
	Gossip
)

func init() {
	updateLocalNodeKey()
}

func SetSDFSChannel(channel chan string) {
	SDFS_CHANNEL = channel
}

// Refresh current node's ID key
func updateLocalNodeKey() {
	localNodeName, err := getLocalNodeAddress()
	if err != nil {
		fmt.Println("Unable to get local network address")
		os.Exit(1)
	}
	LOCAL_NODE_KEY = compressServerTimeID(localNodeName + ":" + time.Now().Format("2017-09-07 17:06:04.000000"))
}

// Update current membership list with incoming list
func updateMembershipList(receivedMembershipList map[string]*Node) {
	if LOCAL_NODE_KEY == "" {
		return
	}
	for key, receivedNode := range receivedMembershipList {
		// In response to being suspected by someone, increase the suspicion incarnation number of self
		if key == LOCAL_NODE_KEY && receivedNode.Status == Suspected {
			customLog(true, "Being suspected, increasing incarnation number")
			self, ok := NodeInfoList[LOCAL_NODE_KEY]
			if !ok {
				fmt.Println("Self is not found in local membership list when updating the list")
				os.Exit(1)
			}
			self.SeqNo++
			continue
		}
		// Do not process suspected node when not in SUSPICION mode
		if !USE_SUSPICION && receivedNode.Status == Suspected {
			continue
		}
		localInfo, ok := NodeInfoList[key]
		// Add the node to membership list if never seen before
		if !ok {
			if receivedNode.Status == Left {
				customLog(true, "Mark node (%v) as LEFT in membership list", key)
			} else {
				customLog(true, "New node (%v) adding it to membership list", key)
			}
			NodeInfoList[key] = receivedNode
			continue
		}
		// The status of a failed node should never be updated after it is deemed FAILED locally
		if localInfo.Status == Failed {
			continue
		}

		// Up to this point, The incoming node statuses can only be either ALIVE or SUSPECTED
		// , and the local statuses for the node can only be either ALIVE or SUSPECTED as well.
		if localInfo.SeqNo < receivedNode.SeqNo {
			localInfo.SeqNo = receivedNode.SeqNo
			localInfo.TimeStamp = time.Now()
			localInfo.Status = receivedNode.Status
			if receivedNode.Status == Left {
				customLog(true, "Mark node (%v) as LEFT in membership list", key)
			}
		}
	}
}

// Encode current node's membership list to protobuf's struct
func nodeInfoListToPB() *pb.NodeInfoList {
	pbNodeList := &pb.NodeInfoList{}
	pbNodeList.Rows = []*pb.NodeInfoRow{}
	for nodeID, nodeInfo := range NodeInfoList {

		var _status pb.NodeInfoRow_NodeStatus
		switch nodeInfo.Status {
		case Alive:
			_status = pb.NodeInfoRow_Alive
		case Suspected:
			// Suspected node is not communcated to peers if self is not using SUSPICION mechanism
			if !USE_SUSPICION {
				continue
			}
			_status = pb.NodeInfoRow_Suspected
		case Left:
			_status = pb.NodeInfoRow_Left
		case Failed:
			// FAILED node should never be communicated to peers over the network
			continue

		}

		pbNodeList.Rows = append(pbNodeList.Rows, &pb.NodeInfoRow{
			NodeID: nodeID,
			SeqNum: nodeInfo.SeqNo,
			Status: _status,
		})
	}

	return pbNodeList
}

// Decode incoming protobuf representation of membership to local representation
func pBToNodeInfoList(incomingNodeList *pb.NodeInfoList) map[string]*Node {
	newNodeList := make(map[string]*Node)
	for _, row := range incomingNodeList.GetRows() {
		var _status StatusType
		switch row.Status {
		case pb.NodeInfoRow_Alive:
			_status = Alive
		case pb.NodeInfoRow_Suspected:
			_status = Suspected
		case pb.NodeInfoRow_Left:
			_status = Left
		case pb.NodeInfoRow_Failed:
			_status = Failed
		}

		newNodeList[row.NodeID] = &Node{
			NodeAddr:  GetAddrFromNodeKey(row.NodeID),
			SeqNo:     row.SeqNum,
			Status:    _status,
			TimeStamp: time.Now(),
		}
	}

	return newNodeList
}

// Construct new message of specified TYPE and local membership list
// *NOTE: The includeLeaderStates variadic parameter should only have length of either 0 or 1.
// *NOTE: The includeLeaderStates parameter should be an optional variable. I used variadics here mainly for backward compatability
func newMessageOfType(messageType pb.GroupMessage_MessageType, includeLeaderStates ...bool) *pb.GroupMessage {
	if len(includeLeaderStates) > 1 {
		panic("Length of includeLeaderStates parameter should never be greater than 1")
	}

	// Only include gossip message
	if len(includeLeaderStates) == 0 {
		return &pb.GroupMessage{
			Type:         messageType,
			NodeInfoList: nodeInfoListToPB(),
		}
	}

	// Gossip message with leader states piggybacked
	if (len(includeLeaderStates) == 1) && includeLeaderStates[0] {
		localHostname, _ := getLocalNodeAddress()
		message := &pb.GroupMessage{
			Type:         messageType,
			NodeInfoList: nodeInfoListToPB(),
			LeaderState: global.LeaderStatesToPB(localHostname),
		}
		return message	
	}

	// Unreachable!
	return nil

}



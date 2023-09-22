package failureDetector

import (
	pb "cs425-mp/protobuf"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	INTRODUCER_ADDRESS  = "fa23-cs425-1801.cs.illinois.edu:55556"
	GOSSIP_RATE         = 500 * time.Millisecond // 500ms
	T_FAIL              = 3 * time.Second        // 3 seconds
	T_SUSPECT           = 2 * time.Second        // 2 seconds
	T_CLEANUP           = 3 * time.Second        // 10 seconds
	NUM_NODES_TO_GOSSIP = 3                      //number of nodes to gossip to
	PORT                = "55556"
	CONN_TIMEOUT        = 500 * time.Millisecond
)

type MessageType int
type StatusType int

const (
	Alive StatusType = iota
	Suspected
	Failed
	Left
)

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

const (
	Join MessageType = iota
	Leave
	Gossip
)

var (
	NodeInfoList      = make(map[string]*Node)
	USE_SUSPICION     = false
	MESSAGE_DROP_RATE = 0.0
	LOCAL_NODE_KEY    = ""
	NodeListLock      = &sync.Mutex{}
)

type Node struct {
	NodeAddr  string
	SeqNo     int32
	Status    StatusType
	TimeStamp time.Time
}

func init() {
	updateLocalNodeKey()
}

func updateLocalNodeKey() {
	localNodeName, err := getLocalNodeAddress()
	if err != nil {
		fmt.Println("Unable to get local network address")
		os.Exit(1)
	}
	LOCAL_NODE_KEY = localNodeName + ":" + time.Now().Format("2017-09-07 17:06:04.000000")
}

// update current membership list with incoming list
func updateMembershipList(receivedMembershipList map[string]*Node) {
	// fmt.Println("Updating membership list with incoming list")
	if LOCAL_NODE_KEY == "" {
		return
	}
	for key, receivedNode := range receivedMembershipList {
		// In response to being suspected by someone, increase the suspicion incarnation number of self
		if key == LOCAL_NODE_KEY && receivedNode.Status == Suspected {
			fmt.Println("Being suspected, increasing incarnation number")
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
			// fmt.Printf("Incrementing counter for node (%v), status: %v -> %v, seqNum: %v -> %v\n", key, localInfo.Status, receivedNode.Status, localInfo.SeqNo, receivedNode.SeqNo)
			localInfo.SeqNo = receivedNode.SeqNo
			localInfo.TimeStamp = time.Now()
			localInfo.Status = receivedNode.Status
			if receivedNode.Status == Left {
				customLog(true, "Mark node (%v) as LEFT in membership list", key)
			}
		}
	}
}

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

func newMessageOfType(messageType pb.GroupMessage_MessageType) *pb.GroupMessage {
	return &pb.GroupMessage{
		Type:         messageType,
		NodeInfoList: nodeInfoListToPB(),
	}
}

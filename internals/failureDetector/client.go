package failureDetector

import (
	"fmt"
	"net"
	"os"
	"time"
)

func StartPeriodicGossipSending() {
	for {
		SendGossip(Gossip)
		time.Sleep(GOSSIP_RATE)
	}
}

// send gossip to other nodes
func SendGossip(msgType MessageType) {
	switch msgType {
	case Join:
		SendJoinMessage()
	case Gossip:
		SendGossipMessage()
	case Leave:
		SendLeaveMessage()
	default:
		fmt.Println("Error: unsupported message type")
		os.Exit(1)
	}
}

func SendJoinMessage() {
	handshakeWithIntroducer()
	//send nodelist to introducer
	conn, err := net.Dial("udp", INTRODUCER_ADDRESS+":"+PORT)
	if err != nil {
		fmt.Println("Error dialing UDP to introducer: ", err)
		return
	}
	defer conn.Close()
	parsedNodes := parseNodeList()
	conn.Write(parsedNodes)
}

func SendGossipMessage() {
	nodeListLock.Lock()
	selectedNodes := randomlySelectNodes(NUM_NODES_TO_GOSSIP)
	if localNode := getLocalNodeFromNodeList(); localNode != nil {
		localNode.HeartbeatCounter++
		localNode.TimeStamp = int(time.Now().Unix())
	}
	parsedNodes := parseNodeList()
	nodeListLock.Unlock()
	sendGossipToNodes(selectedNodes, parsedNodes)
}

func SendLeaveMessage() {
	nodeListLock.Lock()
	selectedNodes := randomlySelectNodes(NUM_NODES_TO_GOSSIP)
	if localNode := getLocalNodeFromNodeList(); localNode != nil {
		localNode.Status = Failed
		localNode.TimeStamp = int(time.Now().Unix())
	}
	gossip := parseLocalNode()
	nodeListLock.Unlock()
	sendGossipToNodes(selectedNodes, gossip)
}

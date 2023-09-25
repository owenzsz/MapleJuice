package failureDetector

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
	pb "cs425-mp/protobuf"
	"google.golang.org/protobuf/proto"
)

// listen to group messages from other nodes and dispatch messages to corresponding handler pipelines
func HandleGroupMessages() {
	conn, err := net.ListenPacket("udp", ":"+PORT)
	if err != nil {
		fmt.Println("Error listening to UDP packets: ", err)
		os.Exit(1)
	}
	defer conn.Close()
	buffer := make([]byte, 4096)
	for {
		// If current node is not initialized or is in LEFT status, do nothing
		if LOCAL_NODE_KEY == "" {
			time.Sleep(GOSSIP_RATE)
			continue
		}
		n, from, err := conn.ReadFrom(buffer)
		if err != nil {
			fmt.Printf("Error reading: %v\n", err.Error())
			continue
		}

		// Drop message based on drop rate
		if shouldDropMessage() {
			continue
		}

		groupMessage := &pb.GroupMessage{}
		err = proto.Unmarshal(buffer[:n], groupMessage)
		if err != nil {
			fmt.Printf("Error unmarshalling group message: %v\n", err.Error())
		}

		switch groupMessage.Type {
		case pb.GroupMessage_JOIN:
			if INTRODUCER_ADDRESS != GetAddrFromNodeKey(LOCAL_NODE_KEY) {
				continue
			}
			processJoinMessage(conn, from, groupMessage)

		case pb.GroupMessage_GOSSIP, pb.GroupMessage_LEAVE:
			go processGossipMessage(groupMessage)
		}
	}
}

// Process the JOIN message and in response give partial membership list back
func processJoinMessage(conn net.PacketConn, from net.Addr, message *pb.GroupMessage) {
	incomingNodeList := pBToNodeInfoList(message.NodeInfoList)
	newcomerKey := message.NodeInfoList.Rows[0].NodeID

	NodeListLock.Lock()
	updateMembershipList(incomingNodeList)
	responseMessage := newResponseToJoin(newcomerKey)
	NodeListLock.Unlock()

	toSend, err := proto.Marshal(responseMessage)
	if err != nil {
		fmt.Printf("Cannot marshalize gossip message: %v\n", err.Error())
	}
	_, err = conn.WriteTo(toSend, from)
	if err != nil {
		fmt.Printf("Failed to write response to JOIN message: %v\n", err.Error())
	}
}

// Process GOSSIP/LEAVE messages, updating membership list as needed
func processGossipMessage(message *pb.GroupMessage) {
	// fmt.Println("Processing gossip message")
	incomingNodeList := pBToNodeInfoList(message.NodeInfoList)
	NodeListLock.Lock()
	updateMembershipList(incomingNodeList)
	NodeListLock.Unlock()
}

// Construct a protobuf message struct using partial membership list
func newResponseToJoin(newcomerKey string) *pb.GroupMessage {
	return &pb.GroupMessage{
		Type:         pb.GroupMessage_GOSSIP,
		NodeInfoList: randomPeersToPB(newcomerKey),
	}
}

// Select random number of peers in the membership list and marshalize to protobuf corresponding struct
func randomPeersToPB(newcomerKey string) *pb.NodeInfoList {
	pbNodeList := &pb.NodeInfoList{}
	pbNodeList.Rows = []*pb.NodeInfoRow{}

	keyPool := make([]string, 0)
	for nodeID := range NodeInfoList {
		if nodeID != newcomerKey {
			keyPool = append(keyPool, nodeID)
		}
	}
	rand.Shuffle(len(keyPool), func(i, j int) { keyPool[i], keyPool[j] = keyPool[j], keyPool[i] })

	numPeersToSend := min(NUM_NODES_TO_GOSSIP, len(keyPool))

	for _, nodeID := range keyPool {
		nodeInfo := NodeInfoList[nodeID]

		var _status pb.NodeInfoRow_NodeStatus
		switch nodeInfo.Status {
		case Alive:
			_status = pb.NodeInfoRow_Alive
		case Suspected, Failed, Left:
			continue
		}

		pbNodeList.Rows = append(pbNodeList.Rows, &pb.NodeInfoRow{
			NodeID: nodeID,
			SeqNum: nodeInfo.SeqNo,
			Status: _status,
		})

		numPeersToSend--
		if numPeersToSend == 0 {
			break
		}
	}

	return pbNodeList
}

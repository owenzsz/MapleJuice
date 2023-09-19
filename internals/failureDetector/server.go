package failureDetector

import (
	"fmt"
	"net"
	"os"

	pb "cs425-mp/protobuf"

	"google.golang.org/protobuf/proto"
)

// listen to group messages from other nodes
func HandleGroupMessages() {
	conn, err := net.ListenPacket("udp", ":"+PORT)
	if err != nil {
		fmt.Println("Error listening to UDP packets: ", err)
		os.Exit(1)
	}
	defer conn.Close()
	buffer := make([]byte, 4096)
	for {
		n, from, err := conn.ReadFrom(buffer)
		if err != nil {
			fmt.Printf("Error reading: %v\n", err.Error())
			os.Exit(1)
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

		case pb.GroupMessage_GOSSIP:
			go processGossipMessage(conn, groupMessage)
			// TODO: add Leave message
		}
	}
}

// Process the JOIN message and in response generate a gossip message for the newcomer
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

func processGossipMessage(conn net.PacketConn, message *pb.GroupMessage) {
	incomingNodeList := pBToNodeInfoList(message.NodeInfoList)
	NodeListLock.Lock()
	updateMembershipList(incomingNodeList)
	NodeListLock.Unlock()
}

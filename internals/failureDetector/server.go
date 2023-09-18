package failureDetector

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

func InitializeNodeList() {
	localNodeName := getLocalNodeName()
	initialNodeList := map[string]*Node{
		LOCAL_NODE_KEY: {
			Address:          localNodeName,
			HeartbeatCounter: 1,
			Status:           Alive,
			TimeStamp:        int(time.Now().Unix()),
		},
	}

	SetNodeList(initialNodeList)
}

func SetNodeList(nodes map[string]*Node) {
	nodeListLock.Lock()
	nodeList = nodes
	nodeListLock.Unlock()
}

// listen to gossip from other nodes
func StartGossipDetector() {
	if hostname, err := os.Hostname(); err == nil && hostname != INTRODUCER_ADDRESS {
		SendGossip(Join)
	}
	go startPerodicFailureCheck()
	go startListeningToJoinMessagesIfNeeded()
	startListeningToGossips()
}

// start periodic failure check
func startPerodicFailureCheck() {
	for {
		nodeListLock.Lock()
		for key, node := range nodeList {
			if key == LOCAL_NODE_KEY {
				continue
			}
			switch node.Status {
			case Alive:
				if time.Now().Unix()-int64(node.TimeStamp) > T_FAIL {
					if USE_SUSPICION {
						fmt.Println("Marking ", key, " as suspected")
						node.Status = Suspected
					} else {
						fmt.Println("Marking ", key, " as failed")
						node.Status = Failed
					}
				}
			case Failed:
				if time.Now().Unix()-int64(node.TimeStamp) > T_CLEANUP {
					fmt.Println("Deleting node: ", key)
					delete(nodeList, key)
				}
			case Suspected:
				if !USE_SUSPICION || time.Now().Unix()-int64(node.TimeStamp) > T_FAIL {
					fmt.Println("Marking ", key, " as failed")
					node.Status = Failed
				}
			}
		}
		nodeListLock.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

func startListeningToGossips() {
	server, err := net.ListenPacket("udp", ":"+PORT)
	if err != nil {
		fmt.Println("Error listening to UDP packets: ", err)
		os.Exit(1)
	}
	defer server.Close()
	buffer := make([]byte, 1024)
	for {
		//accept connection
		n, _, err := server.ReadFrom(buffer)
		if err != nil {
			fmt.Printf("Error reading: %v\n", err.Error())
			os.Exit(1)
		}

		var receivedMembershipList map[string]*Node
		if err := json.Unmarshal(buffer[:n], &receivedMembershipList); err != nil {
			fmt.Println("Error decoding membership list: ", err)
		} else {
			updateMembershipList(receivedMembershipList)
		}
	}
}

func startListeningToJoinMessagesIfNeeded() {
	if hostname, err := os.Hostname(); err == nil && hostname != INTRODUCER_ADDRESS {
		return
	}
	address, err := net.ResolveUDPAddr("udp", INTRODUCER_ADDRESS+":"+INTRODUCER_PORT)
	if err != nil {
		fmt.Println("Error resolving address: ", err)
		return
	}
	conn, err := net.ListenUDP("udp", address)
	if err != nil {
		fmt.Println("Error creating introducer UDP service: ", err)
		return
	}
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		_, clientAddress, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading from UDP: ", err)
			return
		}

		reply := []byte("OK")
		_, err = conn.WriteToUDP(reply, clientAddress)
		if err != nil {
			fmt.Println("Error sending reply:", err)
			return
		}
	}

}

// update current membership list with incoming list
func updateMembershipList(receivedMembershipList map[string]*Node) {
	nodeListLock.Lock()
	for key, receivedNode := range receivedMembershipList {
		if val, ok := nodeList[key]; ok {
			if val.Status == Failed {
				continue
			} else if val.HeartbeatCounter < receivedNode.HeartbeatCounter {
				val.HeartbeatCounter = receivedNode.HeartbeatCounter
				val.TimeStamp = int(time.Now().Unix())
				val.Status = receivedNode.Status
			}
		} else {
			nodeList[key] = receivedNode
		}
	}
	nodeListLock.Unlock()
}

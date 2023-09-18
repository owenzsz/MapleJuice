package failureDetector

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

func sendGossipToNodes(selectedNodes []*Node, gossip []byte) {
	var wg sync.WaitGroup
	for _, node := range selectedNodes {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			rand.Seed(time.Now().UnixNano())
			randomNumber := rand.Float64()
			if randomNumber > 1-MESSAGE_DROP_RATE {
				return
			}
			conn, err := net.Dial("udp", address)
			if err != nil {
				fmt.Println("Error dialing UDP: ", err)
				return
			}
			defer conn.Close()
			_, err = conn.Write(gossip)
			if err != nil {
				fmt.Println("Error sending UDP: ", err)
				return
			}
		}(node.Address)
	}
	wg.Wait()
}

// helper function to randomly select B nodes to gossip to
func randomlySelectNodes(num int) []*Node {
	num = max(num, len(nodeList))
	keys := make([]string, 0, len(nodeList))
	for k := range nodeList {
		keys = append(keys, k)
	}

	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	selectedNodes := make([]*Node, 0, num)
	for i := 0; i < num; i++ {
		selectedNodes = append(selectedNodes, nodeList[keys[i]])
	}
	return selectedNodes
}

func getLocalNodeFromNodeList() *Node {
	return nodeList[LOCAL_NODE_KEY]
}

func getLocalNodeName() string {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Error getting host name: ", err)
		os.Exit(1)
	}
	key := hostname + ":" + PORT
	return key
}

// helper function to calculate the max of two nodes
func max(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// helper function to parse node list into byte array
func parseNodeList() []byte {
	byteSlice, err := json.Marshal(nodeList)
	if err != nil {
		fmt.Println("Error encoding node list to json: ", err)
		return nil
	}

	return byteSlice
}

func parseLocalNode() []byte {
	gossipMap := make(map[string]*Node)
	if value, exists := nodeList[LOCAL_NODE_KEY]; exists {
		gossipMap[LOCAL_NODE_KEY] = value

		byteSlice, err := json.Marshal(gossipMap)
		if err != nil {
			fmt.Println("Error encoding local node to JSON:", err)
			return nil
		}
		return byteSlice
	}
	fmt.Println("Error: local key not found")
	return nil
}

func handshakeWithIntroducer() {
	introducerAddr, err := net.ResolveUDPAddr("udp", INTRODUCER_ADDRESS+":"+INTRODUCER_PORT)
	if err != nil {
		fmt.Println("Error resolving server address:", err)
		return
	}
	conn, err := net.DialUDP("udp", nil, introducerAddr)
	if err != nil {
		fmt.Println("Error dialing UDP to introducer: ", err)
		return
	}
	defer conn.Close()
	buffer := make([]byte, 1024)
	joinRequest := []byte("JOIN")
	for {
		_, err = conn.Write(joinRequest)
		if err != nil {
			fmt.Println("Error sending JOIN message:", err)
			return
		}

		conn.SetReadDeadline(time.Now().Add(1 * time.Second))

		_, _, err = conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading from UDP:", err)
			continue
		}
		break
	}
}

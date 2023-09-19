package failureDetector

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
)

// helper function to randomly select B nodes to gossip to
func randomlySelectNodes(num int) []*Node {
	num = min(num, len(NodeInfoList))
	keys := make([]string, 0, len(NodeInfoList))
	for k := range NodeInfoList {
		keys = append(keys, k)
	}

	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	selectedNodes := make([]*Node, 0, num)
	for i := 0; i < num; i++ {
		selectedNodes = append(selectedNodes, NodeInfoList[keys[i]])
	}
	return selectedNodes
}

// func getLocalNodeFromNodeList() *Node {
// 	return NodeInfoList[LOCAL_NODE_KEY]
// }

func getLocalNodeAddress() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Error getting host name: ", err)
		return "", err
	}
	key := hostname + ":" + PORT
	return key, nil
}

// Given a nodeKey in format of [hostname]:[port]:[timestamp], extract the [hostname]:[port] part as a string
func GetAddrFromNodeKey(nodeKey string) string {
	idSplitted := strings.Split(nodeKey, ":")
	peer_name := idSplitted[0]
	peer_port := idSplitted[1]
	return peer_name + ":" + peer_port
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

package failureDetector

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

// Helper function to randomly select <= NUM nodes to gossip to
func randomlySelectNodes(num int) []*Node {
	num = min(num, len(NodeInfoList)-1)
	keys := make([]string, 0, len(NodeInfoList))
	for k := range NodeInfoList {
		// do not add self to target list
		if k == LOCAL_NODE_KEY {
			continue
		}
		keys = append(keys, k)
	}

	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	selectedNodes := make([]*Node, 0, len(keys))
	for _, nodeKey := range keys {
		nodeInfo := NodeInfoList[nodeKey]
		if nodeInfo.Status == Failed || nodeInfo.Status == Left || nodeInfo.Status == Suspected {
			continue
		}
		selectedNodes = append(selectedNodes, nodeInfo)
		num--
		if num == 0 {
			break
		}
	}
	return selectedNodes
}

// Get local node's ip:port address
func getLocalNodeAddress() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Error getting host name: ", err)
		return "", err
	}
	key := hostname + ":" + PORT
	return key, nil
}

// Compress strings like "fa23-cs425-1805.cs.illinois.edu:55556:22097-09-05 97:23:35.319919" to the format of "05_319919"
func compressServerTimeID(input string) string {
	parts := strings.Split(input, "-")
	serverNumber := parts[2][2:4]
	millisecond := parts[4][len(parts[4])-6:]
	result := fmt.Sprintf("%s_%s", serverNumber, millisecond)
	return result
}

// Decompress the compressed ID. See compressServerTimeID()
func decompressServerTimeID(input string) string {
    parts := strings.Split(input, "_")
    serverNumber := parts[0]
	millisecond := parts[1]
    decompressedID := fmt.Sprintf("fa23-cs425-18%s.cs.illinois.edu:55556:22097-09-05 97:23:35.%s", serverNumber, millisecond)
    return decompressedID
}

// Given a nodeKey in format of [machine_number]_[version_number], extract the [hostname]:[port] as a string
func GetAddrFromNodeKey(nodeKey string) string {
	nodeKey = decompressServerTimeID(nodeKey)
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

// Log setup function. Should call before starting anything
func EnableLog() {
	f, err := os.OpenFile("log.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
		os.Exit(1)
	}

	log.SetOutput(f)

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Cannot get host name ", err.Error())
		os.Exit(1)
	}
	log.SetPrefix(hostname + ": ")
}

// helper function to write log
func customLog(printToStdout bool, format string, v ...interface{}) {
	var mode = ""
	messaegeDropRate := strconv.FormatFloat(MESSAGE_DROP_RATE, 'f', -1, 64)
	if USE_SUSPICION {
		mode = "Gossip + S"
	} else {
		mode = "Gossip"
	}
	msg := fmt.Sprintf(format, v...)
	if printToStdout {
		fmt.Printf("[%v] %s\n", time.Now().Format("2006-01-02 15:04:05"), msg)
	}
	log.Printf("Current Mode[%s]; message drop rate[%s] - %s\n", mode, messaegeDropRate, msg)
}

// Determine whether should drop message based on manually set message drop rate
func shouldDropMessage() bool {
	return rand.Float64() < MESSAGE_DROP_RATE
}

package failureDetector

import (
	pb "cs425-mp/protobuf"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

func PeriodicUpdate() {
	for {
		// If local is not started or is still in LEFT status, do nothing
		if LOCAL_NODE_KEY == "" {
			time.Sleep(GOSSIP_RATE)
			continue
		}
		NodeListLock.Lock()
		gossip := NodeStatusUpdateAndNewGossip()
		selectedNodes := randomlySelectNodes(NUM_NODES_TO_GOSSIP)
		NodeListLock.Unlock()
		SendGossip(gossip, selectedNodes)
		time.Sleep(GOSSIP_RATE)
	}
}

func NodeStatusUpdateAndNewGossip() *pb.GroupMessage {
	// fmt.Println("Checking failure")
	for key, node := range NodeInfoList {
		if key == LOCAL_NODE_KEY {
			node.TimeStamp = time.Now()
			node.SeqNo++
			continue
		}
		sinceLastTimestamp := time.Since(node.TimeStamp)
		switch node.Status {
		case Alive:
			if sinceLastTimestamp > T_FAIL {
				if USE_SUSPICION {
					customLog(true, "Marking %v as suspected", key)
					node.Status = Suspected
					node.TimeStamp = time.Now()
				} else {
					customLog(true, "Marking %v as failed, over time for %v time", key, sinceLastTimestamp.Seconds()-T_FAIL.Seconds())
					node.Status = Failed
					node.TimeStamp = time.Now()
				}
			}
		case Failed, Left:
			if sinceLastTimestamp > T_CLEANUP {
				customLog(true, "Deleting node: %v", key)
				delete(NodeInfoList, key)
			}
		case Suspected:
			if !USE_SUSPICION || sinceLastTimestamp > T_FAIL {
				customLog(true, "Marking %v as failed", key)
				node.Status = Failed
				node.TimeStamp = time.Now()
			}
		}
	}
	gossip := newMessageOfType(pb.GroupMessage_GOSSIP)
	return gossip
}

// send gossip to other nodes
func SendGossip(message *pb.GroupMessage, targets []*Node) {
	// fmt.Println("Sending gossips")
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		fmt.Printf("Failed to marshal GroupMessage: %v\n", err.Error())
	}
	sendGossipToNodes(targets, messageBytes)
}

func sendGossipToNodes(selectedNodes []*Node, gossip []byte) {
	// fmt.Println("Sending gossips to nodes")
	var wg sync.WaitGroup
	for _, node := range selectedNodes {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			// TODO: abstract drop rate detail to a separate method that each send will instead call
			rand.Seed(time.Now().UnixNano())
			randomNumber := rand.Float64()
			if randomNumber > 1-MESSAGE_DROP_RATE {
				return
			}
			conn, err := net.DialTimeout("udp", address, CONN_TIMEOUT)
			if err != nil {
				// fmt.Println("Error dialing UDP: ", err)
				return
			}
			conn.SetWriteDeadline(time.Now().Add(CONN_TIMEOUT))
			defer conn.Close()
			_, err = conn.Write(gossip)
			if err != nil {
				fmt.Println("Error sending UDP: ", err)
				return
			}
			customLog(false, "Sent gossip with size of %v bytes", len(gossip))
		}(node.NodeAddr)
	}
	wg.Wait()
}

func JoinGroupAndInit() error {
	// Populate the first entry in Node List
	selfAddr := GetAddrFromNodeKey(LOCAL_NODE_KEY)
	initialNodeList := map[string]*Node{
		LOCAL_NODE_KEY: {
			NodeAddr:  selfAddr,
			SeqNo:     1,
			Status:    Alive,
			TimeStamp: time.Now(),
		},
	}
	NodeListLock.Lock()
	NodeInfoList = initialNodeList
	NodeListLock.Unlock()
	// INTRODUCER node's setup is done after it has populated its membership list
	if selfAddr == INTRODUCER_ADDRESS {
		return nil
	}

	// Construct JOIN Message
	groupMessage := newMessageOfType(pb.GroupMessage_JOIN)
	msg, err := proto.Marshal(groupMessage)
	if err != nil {
		fmt.Printf("Failed to marshal GroupMessage: %v\n", err.Error())
	}
	// Send out JOIN message
	conn, err := net.Dial("udp", INTRODUCER_ADDRESS)
	if err != nil {
		return err
	}

	defer conn.Close()

	// Try 5 times the join process, if all fail, return err
	for i := 0; i < 5; i++ {
		err = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			return err
		}
		_, err = conn.Write(msg)
		customLog(false, "Sent Join message with size of %v bytes", len(msg))
		if err != nil {
			fmt.Println("Unable to send JOIN message")
			time.Sleep(GOSSIP_RATE)
			continue
		}

		buffer := make([]byte, 4096)
		n, err := conn.Read(buffer)
		if err != nil {
			println("Read data failed:", err.Error())
			time.Sleep(GOSSIP_RATE)
			continue
		}

		pbGroupMessage := &pb.GroupMessage{}
		err = proto.Unmarshal(buffer[:n], pbGroupMessage)
		if err != nil {
			println("Failed to unmarshal GroupMessage" + err.Error())
			return err
		}

		if pbGroupMessage.Type != pb.GroupMessage_GOSSIP {
			println("Received something else other than GOSSIP from INTRODUCER")
			time.Sleep(GOSSIP_RATE)
			continue
		}

		peerRows := pbGroupMessage.NodeInfoList
		newNodeInfoList := pBToNodeInfoList(peerRows)

		// Merge local NodeInfoList ith newNodeInfoList
		NodeListLock.Lock()
		updateMembershipList(newNodeInfoList)
		NodeListLock.Unlock()
		return nil
	}
	return errors.New("failed to join the group after 5 tries")

}

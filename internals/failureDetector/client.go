package failureDetector

import (
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

func PeriodicUpdate() {
	for {
		// If current node is not started or is still in LEFT status, do nothing
		if LOCAL_NODE_KEY == "" {
			time.Sleep(GOSSIP_RATE)
			continue
		}
		// Perform periodic membership refresh and sendout heartbeats
		NodeListLock.Lock()
		gossip := NodeStatusUpdateAndNewGossip()
		selectedNodes := randomlySelectNodes(NUM_NODES_TO_GOSSIP)
		NodeListLock.Unlock()
		SendGossip(gossip, selectedNodes)
		time.Sleep(GOSSIP_RATE)
	}
}

func NodeStatusUpdateAndNewGossip() *pb.GroupMessage {
	for key, node := range NodeInfoList {
		if key == LOCAL_NODE_KEY {
			node.TimeStamp = time.Now()
			node.SeqNo++
			continue
		}
		sinceLastTimestamp := time.Since(node.TimeStamp)
		switch node.Status {
		case Alive:
			if USE_SUSPICION && sinceLastTimestamp > T_SUSPECT {
				customLog(true, "Marking %v as suspected", key)
				node.Status = Suspected
				node.TimeStamp = time.Now()
			} else if sinceLastTimestamp > T_FAIL {
				customLog(true, "Marking %v as failed, over time for %v time", key, sinceLastTimestamp.Seconds()-T_FAIL.Seconds())
				node.Status = Failed
				node.TimeStamp = time.Now()
			}
		case Failed, Left:
			if sinceLastTimestamp > T_CLEANUP {
				customLog(true, "Deleting node: %v", key)
				delete(NodeInfoList, key)
			}
		case Suspected:
			if !USE_SUSPICION || sinceLastTimestamp > T_FAIL {
				if !USE_SUSPICION {
					customLog(true, "Marking %v as failed", key)
				} else if sinceLastTimestamp > T_FAIL {
					customLog(true, "Marking %v as from suspected to failed, over time for %v time", key, sinceLastTimestamp.Seconds()-T_FAIL.Seconds())
				}
				node.Status = Failed
				node.TimeStamp = time.Now()
			}
		}
	}
	gossip := newMessageOfType(pb.GroupMessage_GOSSIP, true)
	return gossip
}

// send gossip to other nodes
func SendGossip(message *pb.GroupMessage, targets []*Node) {
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		fmt.Printf("Failed to marshal GroupMessage: %v\n", err.Error())
	}
	sendGossipToNodes(targets, messageBytes)
}

func sendGossipToNodes(selectedNodes []*Node, gossip []byte) {
	var wg sync.WaitGroup
	for _, node := range selectedNodes {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			// Using DNS cached response as possible
			DNS_Cache_Lock.Lock()
			if cachedAddress, ok := DNS_Cache[address]; ok {
				address = cachedAddress
			} else {
				newAddr, err := net.ResolveUDPAddr("udp", address)
				if err != nil {
					panic("DNS resolve failed")
				}
				newAddrStr := newAddr.String()
				DNS_Cache[address] = newAddrStr
				address = newAddrStr
			}
			DNS_Cache_Lock.Unlock()

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
		}(addPortNumberToNodeAddr(node.NodeAddr))
	}
	wg.Wait()
}

// Join compute cluster without using an Introducer node. Need all machine's address hardcoded in global.go
func JoinGroupAndInitByIntroducer() error {
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

	conn, err := net.Dial("udp", INTRODUCER_ADDRESS+":"+global.FD_PORT)
	if err != nil {
		return err
	}

	defer conn.Close()

	// Try joining the group for five times. If all fail, exit with error
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

// Join compute cluster without using an Introducer node. Need all machine's address hardcoded in global.go
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

	// Construct JOIN Message
	groupMessage := newMessageOfType(pb.GroupMessage_JOIN)
	msg, err := proto.Marshal(groupMessage)
	if err != nil {
		fmt.Printf("Failed to marshal GroupMessage: %v\n", err.Error())
	}

	// We send JOIN message to all peers except local machine and hope at least one machine would respond with its membership list.
	// If failed after trying every peer for 3 times, declare join failed.
	for i := 0; i < 3; i++ {
		for _, peerAddr := range global.SERVER_ADDRS {
			// skip local machine
			if peerAddr == selfAddr {
				continue
			}
			conn, err := net.Dial("udp", peerAddr+":"+global.FD_PORT)
			if err != nil {
				continue
			}
			defer conn.Close()

			err = conn.SetDeadline(time.Now().Add(2 * time.Second))
			if err != nil {
				continue
			}

			// Send JOIN message
			_, err = conn.Write(msg)
			customLog(false, "Sent Join message with size of %v bytes", len(msg))
			if err != nil {
				fmt.Println("Unable to send JOIN message")
				continue
			}

			// Receive GOSSIP message
			buffer := make([]byte, 4096)
			n, err := conn.Read(buffer)
			if err != nil {
				// println("Read data failed:", err.Error())
				continue
			}

			pbGroupMessage := &pb.GroupMessage{}
			err = proto.Unmarshal(buffer[:n], pbGroupMessage)
			if err != nil {
				println("Failed to unmarshal GroupMessage" + err.Error())
				continue
			}

			if pbGroupMessage.Type != pb.GroupMessage_GOSSIP {
				println("Received something else other than GOSSIP from peers")
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
	}

	return errors.New("failed to join the group after 3 tries")

}

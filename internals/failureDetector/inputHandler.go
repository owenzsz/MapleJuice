package failureDetector

import (
	"bufio"
	pb "cs425-mp/protobuf"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func HandleUserInput() {
	inputChan := make(chan string)
	//start another goroutine to keep reading user input in a loop
	go GetUserInputInLoop(inputChan)
	//process and read user input. The two goroutines will communicate via a channel
	ProcessUserInputInLoop(inputChan)
}

func GetUserInputInLoop(inputChan chan<- string) {
	for {
		reader := bufio.NewReader(os.Stdin)

		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("An error occurred while reading input. Please try again", err)
			return
		}
		//send the user input to inputChan
		inputChan <- input
	}
}

func ProcessUserInputInLoop(inputChan <-chan string) {
	for {
		//read the user input from inputChan
		query := <-inputChan
		switch strings.TrimRight(query, "\n") {
		case "leave":
			handleLeave()
		case "rejoin":
			handleRejoin()
		case "list_mem":
			showMembershipList()
		case "list_self":
			showSelfID()
		default:
			fmt.Println("Error: input command not supported.")
		}

	}
}

func handleLeave() {
	if LOCAL_NODE_KEY == "" {
		fmt.Println("Error: cannot leave when the current node does not exist in the network")
		return
	}
	NodeListLock.Lock()
	NodeInfoList[LOCAL_NODE_KEY].Status = Left
	NodeInfoList[LOCAL_NODE_KEY].SeqNo++
	leaveMessage := newMessageOfType(pb.GroupMessage_LEAVE)
	selectedNodes := randomlySelectNodes(NUM_NODES_TO_GOSSIP)
	SendGossip(leaveMessage, selectedNodes)
	NodeInfoList = nil
	LOCAL_NODE_KEY = ""
	NodeListLock.Unlock()
}

func handleRejoin() {
	if LOCAL_NODE_KEY != "" {
		fmt.Println("Error: cannot rejoin when the current node already exists in some networks")
		return
	}
	updateLocalNodeKey()
	err := JoinGroupAndInit()
	if err != nil {
		fmt.Printf("Cannot join the group: %v\n", err.Error())
		return
	}
}

func showMembershipList() {
	NodeListLock.Lock()
	currList := make(map[string]Node)
	for key, value := range NodeInfoList {
		currList[key] = *value
	}
	NodeListLock.Unlock()

	list, err := json.MarshalIndent(currList, "", "  ")
	if err != nil {
		fmt.Println("failed to pretty-print the membership list")
	}
	fmt.Println("***************** Current Membership List *****************")
	fmt.Println(string(list))
	fmt.Println("***********************************************************")
}

func showSelfID() {
	NodeListLock.Lock()
	id := LOCAL_NODE_KEY
	NodeListLock.Unlock()

	fmt.Println(">>> ", id)
}

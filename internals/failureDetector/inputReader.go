package failureDetector

import (
	"bufio"
	pb "cs425-mp/protobuf"
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
	NodeInfoList[LOCAL_NODE_KEY].Status = Failed
	NodeInfoList[LOCAL_NODE_KEY].SeqNo++
	leaveMessage := newMessageOfType(pb.GroupMessage_LEAVE)
	SendGossip(leaveMessage)
	NodeInfoList = nil
	NodeListLock.Unlock()
	LOCAL_NODE_KEY = ""
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

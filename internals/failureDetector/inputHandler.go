package failureDetector

import (
	"bufio"
	pb "cs425-mp/protobuf"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
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
		trimmed := strings.TrimRight(query, "\n")
		splitted := strings.Split(trimmed, " ")
		if len(splitted) == 1 {
			switch splitted[0] {
			case "leave":
				HandleLeave()
			case "rejoin":
				HandleRejoin()
			case "list_mem":
				ShowMembershipList()
			case "list_self":
				ShowSelfID()
			case "enable_suspicion":
				ToggleSuspicion(true)
			case "disable_suspicion":
				ToggleSuspicion(false)
			default:
				fmt.Println("Error: input command not supported.")
			}
		} else if len(splitted) == 2 {
			switch splitted[0] {
			case "drop_rate":
				AdjustDropRate(splitted[1])
			default:
				fmt.Println("Error: input command not supported.")
			}

		} else {
			fmt.Println("Error: input command not supported.")
		}

	}
}

func HandleLeave() {
	if LOCAL_NODE_KEY == "" {
		fmt.Println("Error: cannot leave when the current node does not exist in the network")
		return
	}
	NodeListLock.Lock()
	NodeInfoList[LOCAL_NODE_KEY].Status = Left
	NodeInfoList[LOCAL_NODE_KEY].SeqNo++
	leaveMessage := newMessageOfType(pb.GroupMessage_LEAVE)
	selectedNodes := RandomlySelectNodes(NUM_NODES_TO_GOSSIP, LOCAL_NODE_KEY)
	SendGossip(leaveMessage, selectedNodes)
	NodeInfoList = nil
	LOCAL_NODE_KEY = ""
	NodeListLock.Unlock()
}

func HandleRejoin() {
	if LOCAL_NODE_KEY != "" {
		fmt.Println("Error: cannot rejoin when the current node already exists in some networks")
		return
	}
	updateLocalNodeKey()
	err := JoinGroupAndInitByIntroducer()
	if err != nil {
		fmt.Printf("Cannot join the group: %v\n", err.Error())
		return
	}
}

func ShowMembershipList() {
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

func ShowSelfID() {
	NodeListLock.Lock()
	id := LOCAL_NODE_KEY
	NodeListLock.Unlock()

	fmt.Println(">>> ", id)
}

func AdjustDropRate(_dropRate string) {
	// VM dns names
	VMs := []string{
		"fa23-cs425-1801.cs.illinois.edu", "fa23-cs425-1802.cs.illinois.edu",
		"fa23-cs425-1803.cs.illinois.edu", "fa23-cs425-1804.cs.illinois.edu",
		"fa23-cs425-1805.cs.illinois.edu", "fa23-cs425-1806.cs.illinois.edu",
		"fa23-cs425-1807.cs.illinois.edu", "fa23-cs425-1808.cs.illinois.edu",
		"fa23-cs425-1809.cs.illinois.edu", "fa23-cs425-1810.cs.illinois.edu",
	}

	Port := 8080

	dropRate, err := strconv.ParseFloat(_dropRate, 64)
	if err != nil || dropRate < 0.0 || dropRate > 1.0 {
		fmt.Println("Not a valid drop rate. Should be between 0.0 and 1.0")
		return
	}

	for _, server := range VMs {
		url := fmt.Sprintf("http://%s:%d/updateMessageDropRate?value=%f", server, Port, dropRate)
		response, err := http.Get(url)
		if err != nil {
			fmt.Printf("Unable to connect to %s\n", server)
			continue
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			fmt.Printf("Error reading response body from %s\n", server)
			continue
		}

		fmt.Printf("%s: %s\n", server, string(body))
	}
}

func ToggleSuspicion(isOn bool) {
	// VM dns names
	VMs := []string{
		"fa23-cs425-1801.cs.illinois.edu", "fa23-cs425-1802.cs.illinois.edu",
		"fa23-cs425-1803.cs.illinois.edu", "fa23-cs425-1804.cs.illinois.edu",
		"fa23-cs425-1805.cs.illinois.edu", "fa23-cs425-1806.cs.illinois.edu",
		"fa23-cs425-1807.cs.illinois.edu", "fa23-cs425-1808.cs.illinois.edu",
		"fa23-cs425-1809.cs.illinois.edu", "fa23-cs425-1810.cs.illinois.edu",
	}

	Port := 8080

	for _, server := range VMs {
		var url string
		if isOn {
			url = fmt.Sprintf("http://%s:%d/enableSuspicion", server, Port)
		} else {
			url = fmt.Sprintf("http://%s:%d/disableSuspicion", server, Port)
		}
		response, err := http.Get(url)
		if err != nil {
			fmt.Printf("Unable to connect to %s\n", server)
			continue
		}
		defer response.Body.Close()

		body, err := io.ReadAll(response.Body)
		if err != nil {
			fmt.Printf("Error reading response body from %s\n", server)
			continue
		}

		fmt.Printf("%s: %s\n", server, string(body))
	}
}

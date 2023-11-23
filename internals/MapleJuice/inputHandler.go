package maplejuice

import (
	"bufio"
	sdfs "cs425-mp/internals/SDFS"
	fd "cs425-mp/internals/failureDetector"
	"fmt"
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
		trimmed := strings.TrimSpace(strings.TrimRight(query, "\n"))
		splitted := strings.Split(trimmed, " ")
		command := splitted[0]
		if command == "leave" {
			fd.HandleLeave()
		} else if command == "list_mem" {
			fd.ShowMembershipList()
		} else if command == "list_self" {
			fd.ShowSelfID()
		} else if command == "enable_suspicion" {
			fd.ToggleSuspicion(true)
		} else if command == "disable_suspicion" {
			fd.ToggleSuspicion(false)
		} else if command == "put" {
			if len(splitted) != 3 {
				fmt.Printf("Expected 3 components for put command, but got %v \n", len(splitted))
				continue
			}
			localFileName := splitted[1]
			sdfsFileName := splitted[2]
			sdfs.HandlePutFile(localFileName, sdfsFileName)
		} else if command == "get" {
			if len(splitted) != 3 {
				fmt.Printf("Expected 3 components for get command, but got %v \n", len(splitted))
				continue
			}
			sdfsFileName := splitted[1]
			localFileName := splitted[2]
			sdfs.HandleGetFile(sdfsFileName, localFileName)
		} else if command == "delete" {
			if len(splitted) != 2 {
				fmt.Printf("Expected 2 components for delete command, but got %v \n", len(splitted))
				continue
			}
			sdfsFileName := splitted[1]
			sdfs.HandleDeleteFile(sdfsFileName)
		} else if command == "ls" {
			if len(splitted) != 2 {
				fmt.Printf("Expected 2 components for ls command, but got %v \n", len(splitted))
				continue
			}
			sdfsFileName := splitted[1]
			sdfs.HandleListFileHolders(sdfsFileName)
		} else if command == "store" {
			if len(splitted) != 1 {
				fmt.Printf("Expected 1 components for store command, but got %v \n", len(splitted))
				continue
			}
			sdfs.HandleListLocalFiles()
		} else if command == "multiread" {
			if len(splitted) <= 2 {
				fmt.Printf("Expected at least 1 target VMs for multiread command\n")
				continue
			}
			sdfsFileName := splitted[1]
			localFileName := splitted[2]
			targetVMs := splitted[3:]
			sdfs.LaunchMultiReads(sdfsFileName, localFileName, targetVMs)
		} else if command == "multiwrite" {
			sdfsFileName := splitted[1]
			localFileName := splitted[2]
			writers := splitted[3:]
			sdfs.LaunchMultiWriteRead(sdfsFileName, localFileName, writers)
		} else if command == "append" {
			sdfsFileName := splitted[1]
			content := splitted[2]
			sdfs.HandleAppendFile(sdfsFileName, content)
		} else if command == "maple" {
			mapleExe := splitted[1]
			numMaples, err := strconv.Atoi(splitted[2])
			if err != nil {
				fmt.Println("Error parsing numMaples", err)
			}
			sdfsIntermediateFileNamePrefix := splitted[3]
			sdfsSrcDirectory := splitted[4]
			handleMaple(mapleExe, numMaples, sdfsIntermediateFileNamePrefix, sdfsSrcDirectory)
		} else if command == "juice" {
			juiceExe := splitted[1]
			numJuices, err := strconv.Atoi(splitted[2])
			if err != nil {
				fmt.Println("Error parsing numJuices", err)
			}
			sdfsIntermediateFileNamePrefix := splitted[3]
			sdfsDestFileName := splitted[4]
			deleteInput := splitted[5] == "1"
			isRangePartition := splitted[6] == "1" // 1 means using range partition, other means using hash partition
			handleJuice(juiceExe, numJuices, sdfsIntermediateFileNamePrefix, sdfsDestFileName, deleteInput, isRangePartition)

		} else {
			fmt.Println("Command Not Supported")
		}

	}
}

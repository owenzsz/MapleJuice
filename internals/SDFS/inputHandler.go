package SDFS

import (
	"bufio"
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
		trimmed := strings.TrimRight(query, "\n")
		splitted := strings.Split(trimmed, " ")
		command := splitted[0]
		if command == "put" {
			if len(splitted) != 3 {
				fmt.Printf("Expected 3 components for put command, but got %v \n", len(splitted))
				continue
			}
			localFileName := splitted[1]
			sdfsFileName := splitted[2]
			handlePutFile(localFileName, sdfsFileName)
		} else if command == "get" {
			if len(splitted) != 3 {
				fmt.Printf("Expected 3 components for get command, but got %v \n", len(splitted))
				continue
			}
			sdfsFileName := splitted[1]
			localFileName := splitted[2]
			handleGetFile(sdfsFileName, localFileName)
		} else if command == "delete" {
			if len(splitted) != 2 {
				fmt.Printf("Expected 2 components for delete command, but got %v \n", len(splitted))
				continue
			}
			sdfsFileName := splitted[1]
			handleDeleteFile(sdfsFileName)
		} else if command == "ls" {
			if len(splitted) != 2 {
				fmt.Printf("Expected 2 components for ls command, but got %v \n", len(splitted))
				continue
			}
			sdfsFileName := splitted[1]
			handleListFileHolders(sdfsFileName)
		} else if command == "store" {
			if len(splitted) != 1 {
				fmt.Printf("Expected 1 components for store command, but got %v \n", len(splitted))
				continue
			}
			handleListLocalFiles()
		} else if command == "multiread" {
			if len(splitted) <= 2 {
				fmt.Printf("Expected at least 1 target VMs for multithread command\n")
				continue
			}
			sdfsFileName := splitted[1]
			localFileName := splitted[2]
			targetVMs := splitted[3:]
			launchMultiReads(sdfsFileName, localFileName, targetVMs)
		} else {
			fmt.Println("Command Not Supported")
		}

	}
}

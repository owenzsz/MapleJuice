package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"cs425-mp1/internals/client"
	"cs425-mp1/internals/server"
)

var serverAddrs = []string{"localhost:55555"}

func getUserInputInLoop(inputChan chan<- string) {
	fmt.Println(">>> Enter Query: ")
	for {
		reader := bufio.NewReader(os.Stdin)

		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("An error occurred while reading input. Please try again", err)
			return
		}

		inputChan <- input
	}
}

func processUserInputInLoop(inputChan <-chan string) {
	for {
		query := <-inputChan
		var wg sync.WaitGroup
		for _, address := range serverAddrs {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				client.RemoteQueryAndPrint(addr, query)
			}(address)
		}
		wg.Wait()
		fmt.Println(">>> Enter Query: ")
	}
}

func main() {
	fmt.Println("App started")

	var wg sync.WaitGroup

	// 1. Start log server
	wg.Add(1)
	go func() {
		defer wg.Done()
		server.Start()
	}()

	// 2. Start waiting for and processing user input
	inputChan := make(chan string)
	wg.Add(2)
	go func() {
		defer wg.Done()
		getUserInputInLoop(inputChan)
	}()
	go func() {
		defer wg.Done()
		processUserInputInLoop(inputChan)
	}()

	// Wait indefinitely until Ctrl-C
	wg.Wait()
}

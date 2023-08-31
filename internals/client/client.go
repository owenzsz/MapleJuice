package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
)

var serverAddrs = []string{"localhost:8080"}

func Start() {
	inputChan := make(chan string)
	go GetUserInputInLoop(inputChan)
	ProcessUserInputInLoop(inputChan)
}

func GetUserInputInLoop(inputChan chan<- string) {
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

func ProcessUserInputInLoop(inputChan <-chan string) {
	for {
		query := <-inputChan
		var wg sync.WaitGroup
		for _, address := range serverAddrs {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				RemoteQueryAndPrint(addr, query)
			}(address)
		}
		wg.Wait()
		fmt.Println(">>> Enter Query: ")
	}
}

func RemoteQueryAndPrint(server string, query string) {
	conn, err := net.Dial("tcp", server)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer conn.Close()

	request := []byte(query)

	_, err = conn.Write(request)
	if err != nil {
		fmt.Printf("Error sending remote query: %v\n", err.Error())
		return
	}

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading response:", err)
			return
		}
		fmt.Printf("Result from [%s] -- %s\n", server, line)
	}
}

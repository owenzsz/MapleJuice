package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

var (
	SERVER_ADDRS = []string{
		"fa23-cs425-1801.cs.illinois.edu", "fa23-cs425-1802.cs.illinois.edu",
		"fa23-cs425-1803.cs.illinois.edu", "fa23-cs425-1804.cs.illinois.edu",
		"fa23-cs425-1805.cs.illinois.edu", "fa23-cs425-1806.cs.illinois.edu",
		"fa23-cs425-1807.cs.illinois.edu", "fa23-cs425-1808.cs.illinois.edu",
		"fa23-cs425-1809.cs.illinois.edu", "fa23-cs425-1810.cs.illinois.edu"}

	PORT = "55555"
)

type Stat struct {
	response   string
	numEntries int
}

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

		stats := make([]Stat, len(SERVER_ADDRS))

		// Spawn goroutines for parallel querying
		var wg sync.WaitGroup
		startTime := time.Now()
		for i, address := range SERVER_ADDRS {
			wg.Add(1)
			go func(__i int, addr string) {
				defer wg.Done()
				stats[__i] = RemoteQueryAndPrint(addr+":"+PORT, query)
			}(i, address)
		}
		wg.Wait()

		// Concatenate gathered statistics and print out them
		fmt.Println("\n========================== Statistics ==========================")
		totalNumEntries := 0
		latency := time.Since(startTime).Milliseconds()
		for _, stat := range stats {
			if stat.numEntries == 0 {
				continue
			}
			fmt.Println(stat.response)
			totalNumEntries += stat.numEntries
		}
		fmt.Printf("In total, fetched %v matched line(s). End-to-end latency: %v ms\n", totalNumEntries, latency)
		fmt.Println("\n>>> Enter Query: ")
	}
}

func RemoteQueryAndPrint(server string, query string) Stat {
	conn, err := net.DialTimeout("tcp", server, 1*time.Second)
	if err != nil {
		// fmt.Println("Error connecting:", err)
		return Stat{"", 0}
	}
	defer conn.Close()

	request := []byte(query)

	_, err = conn.Write(request)
	if err != nil {
		fmt.Printf("Error sending remote query: %v\n", err.Error())
		return Stat{"", 0}
	}

	numEntries := 0
	var latencyReport string
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading response:", err)
			return Stat{"", 0}
		}
		if (len(line) >= 5 && string(line[:5]) == "Error") ||
			(len(line) == 1 && line[0] == '\n') {
			break
		}

		
		if (len(line) >= 5 && string(line[:5]) == "Query") {
			latencyReport = line[:len(line)-1] // discarding the last \n character
		} else {
			numEntries++
			fmt.Printf("<<< Result from [%s] -- %s", server, line)
		}
	}

	stat := fmt.Sprintf("from %s --- %v line(s) matched. Latency: %s", server, numEntries, latencyReport)
	return Stat{stat, numEntries}
}

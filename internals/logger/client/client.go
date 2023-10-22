package client

import (
	"bufio"
	"cs425-mp/internals/global"
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
)

type Stat struct {
	response   string
	numEntries int
}

func Start() {
	inputChan := make(chan string)
	//start another goroutine to keep reading user input in a loop
	go GetUserInputInLoop(inputChan)
	//process and read user input. The two goroutines will communicate via a channel
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
		//send the user input to inputChan
		inputChan <- input
	}
}

func ProcessUserInputInLoop(inputChan <-chan string) {
	for {
		//read the user input from inputChan
		query := <-inputChan

		stats := make([]Stat, len(SERVER_ADDRS))

		// Spawn goroutines for parallel querying
		var wg sync.WaitGroup // need to wait until all requests are received to proceed to printing summary stats
		startTime := time.Now()
		for i, address := range SERVER_ADDRS {
			wg.Add(1)
			go func(__i int, addr string) {
				defer wg.Done()
				//send query to all VMs
				stats[__i] = RemoteQueryAndPrint(addr+":"+global.LOGGER_PORT, query)
			}(i, address)
		}
		wg.Wait() //barrier

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
		//process program-defined error using string matching (i.e. invalid requests)
		if (len(line) >= 5 && string(line[:5]) == "Error") ||
			(len(line) == 1 && line[0] == '\n') {
			break
		}
		//process latency report using string matching
		if len(line) >= 5 && string(line[:5]) == "Query" {
			latencyReport = line[:len(line)-1] // discarding the last \n character
		} else {
			numEntries++
			fmt.Printf("<<< Result from [%s] -- %s", server, line)
		}
	}

	stat := fmt.Sprintf("from %s --- %v line(s) matched. Latency: %s", server, numEntries, latencyReport)
	return Stat{stat, numEntries}
}

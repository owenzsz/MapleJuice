package server

import (
	"bufio"
	"cs425-mp/internals/global"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	HOST = "0.0.0.0"
)

func Start() {
	//start monitoring requests on port 55555
	server, err := net.Listen("tcp", HOST+":"+global.LOGGER_PORT)
	if err != nil {
		fmt.Printf("Error listening: %v\n", err.Error())
		os.Exit(1)
	}
	defer server.Close()
	for {
		//accept connection
		conn, err := server.Accept()
		if err != nil {
			fmt.Printf("Error accepting: %v\n", err.Error())
			os.Exit(1)
		}
		//start another goroutine to process information sent to conn
		go serveConn(conn)
	}
}

func serveConn(conn net.Conn) {
	defer conn.Close()
	//read content from conn
	reader := bufio.NewReader(conn)
	request, err := reader.ReadString('\n')
	if err != nil {
		if err != io.EOF {
			fmt.Printf("Error reading request: %v\n", err.Error())
		}
		return
	}
	request = strings.TrimSuffix(request, "\n")
	//process the request
	response := processRequest(request)
	//write the response to client via conn
	_, err = conn.Write(response)
	if err != nil {
		fmt.Printf("Error sending response: %v\n", err.Error())
		return
	}
}

func processRequest(request string) []byte {
	fmt.Println("Received request: ", request)
	requestComponents := strings.Split(request, " ")
	//for MP1, we won't handle any other things excep for grep
	if requestComponents[0] != "grep" {
		return []byte("Error: Invalid request. Please try again.\n")
	}
	startTime := time.Now()
	//execute the requested command on current server
	cmd := exec.Command("bash", "-c", request)
	out, err := cmd.Output()
	if err != nil {
		return []byte("Error: Error executing command.\n")
	}

	latency := time.Since(startTime).Milliseconds()

	latencyReport := []byte(fmt.Sprintf("Query took %v ms\n", latency))
	out = append(out, latencyReport...)
	out = append(out, '\n')
	return out
}

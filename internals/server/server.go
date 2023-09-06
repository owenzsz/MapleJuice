package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
)

const (
	HOST     = "0.0.0.0"
	PORT     = "8080"
	LOG_FILE = "fake_log.log"
)

func Start() {
	server, err := net.Listen("tcp", HOST+":"+PORT)
	if err != nil {
		fmt.Printf("Error listening: %v\n", err.Error())
		os.Exit(1)
	}
	defer server.Close()
	for {
		conn, err := server.Accept()
		if err != nil {
			fmt.Printf("Error accepting: %v\n", err.Error())
			os.Exit(1)
		}
		go serveConn(conn)
	}
}

func serveConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	request, err := reader.ReadString('\n')
	if err != nil {
		if err != io.EOF {
			fmt.Printf("Error reading request: %v\n", err.Error())
		}
		return
	}
	request = strings.TrimSuffix(request, "\n")

	response := processRequest(request)
	_, err = conn.Write(response)
	if err != nil {
		fmt.Printf("Error sending response: %v\n", err.Error())
		return
	}
}

func processRequest(request string) []byte {
	fmt.Println("Received request: ", request)
	requestComponents := strings.Split(request, " ")
	if requestComponents[0] != "grep" {
		return []byte("Error: Invalid request. Please try again.\n")
	}
	cmd := exec.Command("bash", "-c", request)
	out, err := cmd.Output()
	if err != nil {
		return []byte("Error: Error executing command.\n")
	}

	return append(out, '\n')
}

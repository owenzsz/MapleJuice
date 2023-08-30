package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
)

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

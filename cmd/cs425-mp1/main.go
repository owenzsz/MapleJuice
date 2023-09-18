package main

import (
	"fmt"
	"sync"

	"cs425-mp/internals/logger/client"
	"cs425-mp/internals/logger/server"
)

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
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.Start()
	}()

	// Wait indefinitely until Ctrl-C
	wg.Wait()
}

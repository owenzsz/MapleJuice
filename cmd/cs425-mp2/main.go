package main

import (
	"cs425-mp/internals/failureDetector"
	"fmt"
	"sync"
)

func main() {
	fmt.Println("Failure Detector Started")
	failureDetector.InitializeNodeList()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		failureDetector.HandleRequests()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		failureDetector.StartGossipDetector()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		failureDetector.StartPeriodicGossipSending()
	}()

	wg.Wait()

}

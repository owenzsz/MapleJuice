package main

import (
	"cs425-mp/internals/SDFS"
	"cs425-mp/internals/failureDetector"
	"cs425-mp/internals/global"
	"fmt"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	inputChan := make(chan string)
	failureDetector.SetSDFSChannel(inputChan)
	SDFS.SetFDChannel(inputChan)
	go startFailureDetector(&wg)
	startSDFS(&wg)
	startLeaderElection(&wg)
	go monitorLeader()

	wg.Wait()

}

func startFailureDetector(wg *sync.WaitGroup) {
	fmt.Println("Failure Detector Started")
	// Enable logging
	failureDetector.EnableLog()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Handle http requests for suspicion toggle and drop rate adjustment
		failureDetector.HandleExternalSignals()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Handle group messages
		failureDetector.HandleGroupMessages()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Periodic local state and memebrship refresh
		failureDetector.PeriodicUpdate()
	}()

	// Join the group and initialize local states
	for err := failureDetector.JoinGroupAndInit(); err != nil; err = failureDetector.JoinGroupAndInit() {
		fmt.Printf("Not yet joined the cluster: %v\n", err.Error())
	}
	fmt.Println("Successfully joined cluster!")
}

func startSDFS(wg *sync.WaitGroup) {
	wg.Add(1)
	fmt.Println("SDFS Started")
	go func() {
		defer wg.Done()
		SDFS.HandleUserInput()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		SDFS.ObserveFDChannel()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		SDFS.StartSDFSServer()
	}()
}

func startLeaderElection(wg *sync.WaitGroup) {
	wg.Add(1)
	fmt.Println("Leader Election running in background")
	go func() {
		defer wg.Done()
		SDFS.StartLeaderElection()
	}()
}

func monitorLeader() {
	for {
		leaderID := global.GetLeaderID()
		if leaderID != -1 {
			fmt.Printf("*** Current Leader is VM %v ***\n", leaderID)
		} else {
			fmt.Println("*** Current Leader is NIL ***")
		}
		time.Sleep(5 * time.Second)
	}
}

package main

import (
	"cs425-mp/internals/SDFS"
	"cs425-mp/internals/failureDetector"
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
	// Join the group and initialize local states
	err := failureDetector.JoinGroupAndInit()
	if err != nil {
		fmt.Printf("Cannot join the group: %v\n", err.Error())
		return
	}

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
		SDFS.HandleSDFSMessages()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		SDFS.ObserveFDChannel()
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
		leaderID := SDFS.GetLeaderID()
		if leaderID != -1 {
			fmt.Printf("*** Current Leader is VM %v ***\n", leaderID)
		} else {
			fmt.Println("*** Current Leader is NIL ***")
		}
		time.Sleep(5*time.Second)
	}
}

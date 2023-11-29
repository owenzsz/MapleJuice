package main

import (
	maplejuice "cs425-mp/internals/MapleJuice"
	"cs425-mp/internals/SDFS"
	"cs425-mp/internals/failureDetector"
	"cs425-mp/internals/global"
	"fmt"
	"sync"
	"time"
)

func main() {
	global.MP_NUMBER = 4
	var wg sync.WaitGroup
	go startFailureDetector(&wg)
	startSDFS(&wg)
	startLeaderElection(&wg)
	startMapleJuice(&wg)
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
	fmt.Println("SDFS Started")
	wg.Add(1)
	go func() {
		defer wg.Done()
		SDFS.StartSDFSServer()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		SDFS.PeriodicLeaderTasks()
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
	last_leader := global.GetLeaderID()
	for {

		cur_leader := global.GetLeaderID()
		if cur_leader != last_leader {
			if cur_leader != -1 {
				fmt.Printf("*** Current Leader is VM %v ***\n", cur_leader)
			} else {
				fmt.Println("*** Current Leader is NIL ***")
			}
			last_leader = cur_leader
		}

		time.Sleep(5 * time.Second)
	}
}

func startMapleJuice(wg *sync.WaitGroup) {
	fmt.Println("MapleJuice Started")
	wg.Add(1)
	go func() {
		defer wg.Done()
		maplejuice.HandleUserInput()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		maplejuice.StartMapleJuiceServer()
	}()
}

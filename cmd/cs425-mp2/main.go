package main

import (
	"cs425-mp/internals/failureDetector"
	"fmt"
	"sync"
)

func main() {
	fmt.Println("Failure Detector Started")
	err := failureDetector.JoinGroupAndInit()
	if err != nil {
		fmt.Printf("Cannot join the group: %v\n", err.Error())
		return
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		failureDetector.HandleExternalSignals()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		failureDetector.HandleGroupMessages()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		failureDetector.PeriodicUpdate()
	}()

	wg.Wait()

}

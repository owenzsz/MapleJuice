package main

import (
	"cs425-mp/internals/failureDetector"
	"fmt"
	"log"
	"os"
	"sync"
)

func main() {
	fmt.Println("Failure Detector Started")
	f, err := os.OpenFile("log.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
		os.Exit(1)
	}
	defer f.Close()

	log.SetOutput(f)

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Cannot get host name ", err.Error())
		os.Exit(1)
	}
	log.SetPrefix(hostname + ": ")
	err = failureDetector.JoinGroupAndInit()
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		failureDetector.HandleUserInput()
	}()

	wg.Wait()

}

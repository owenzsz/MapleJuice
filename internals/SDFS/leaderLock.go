package SDFS

import (
	"cs425-mp/internals/global"
	"fmt"
)

func requestLock(requestorAddress string, fileName string, requestType global.RequestType) bool {
	global.GlobalFileLock.Lock()
	lock, exists := global.FileLocks[fileName]
	if !exists {
		lock = &global.FileLock{}
		global.FileLocks[fileName] = lock
	}
	global.GlobalFileLock.Unlock()

	lock.FileLocksMutex.Lock()
	defer lock.FileLocksMutex.Unlock()
	var canProceed bool
	switch requestType {
	case global.READ:
		if !global.Contains(lock.ReadQueue, requestorAddress) {
			lock.ReadQueue = append(lock.ReadQueue, requestorAddress)
		}
		canProceed = lock.WriteCount == 0 && lock.ReadCount < 2 && (len(lock.WriteQueue) == 0 || lock.ConsecutiveReads < 4) && (lock.ReadQueue[0] == requestorAddress || lock.ReadQueue[1] == requestorAddress)
	case global.WRITE:
		if !global.Contains(lock.WriteQueue, requestorAddress) {
			lock.WriteQueue = append(lock.WriteQueue, requestorAddress)
		}
		canProceed = lock.WriteCount == 0 && lock.ReadCount == 0 && (len(lock.ReadQueue) == 0 || lock.ConsecutiveWrites < 4) && lock.WriteQueue[0] == requestorAddress
	}
	if canProceed {
		if requestType == global.READ {
			fmt.Printf("Granted read lock for file %s\n", fileName)
			lock.ReadCount++
			lock.ConsecutiveReads++
			lock.ConsecutiveWrites = 0
		} else {
			fmt.Printf("Granted write lock for file %s\n", fileName)
			lock.WriteCount++
			lock.ConsecutiveWrites++
			lock.ConsecutiveReads = 0
		}
	}
	return canProceed
}

func releaseLock(requesterAddress string, fileName string, requestType global.RequestType) {
	global.GlobalFileLock.Lock()
	lock, exists := global.FileLocks[fileName]
	global.GlobalFileLock.Unlock()
	if !exists {
		return
	}
	lock.FileLocksMutex.Lock()
	defer lock.FileLocksMutex.Unlock()
	if requestType == global.READ {
		if lock.ReadCount <= 0 || len(lock.ReadQueue) <= 0 {
			fmt.Println("Error: Read count or read queue is empty while unlocking read lock")
			return
		}
		fmt.Printf("Released read lock for file %s\n", fileName)
		lock.ReadCount--
		newReadQueue, err := global.RemoveElementFromFirstTwo(lock.ReadQueue, requesterAddress)
		if err != nil {
			fmt.Printf("Error dequeing read queue: %s\n", err.Error())
		} else {
			lock.ReadQueue = newReadQueue
		}
	} else {
		if lock.WriteCount <= 0 || len(lock.WriteQueue) <= 0 {
			fmt.Println("Error: Write count or write queue is empty while unlocking write lock")
			return
		}
		fmt.Printf("Released write lock for file %s\n", fileName)
		lock.WriteCount--
		lock.WriteQueue = lock.WriteQueue[1:]
	}
}

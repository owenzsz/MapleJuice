package SDFS

import (
	"fmt"
	"cs425-mp/internals/global"
)



func requestLock(requestorAddress string, fileName string, requestType global.RequestType) {
	global.GlobalFileLock.Lock()
	lock, exists := global.FileLocks[fileName]
	if !exists {
		lock = &global.FileLock{}
		global.FileLocks[fileName] = lock
	}
	global.GlobalFileLock.Unlock()

	lock.FileLocksMutex.Lock()
	defer lock.FileLocksMutex.Unlock()
	if requestType == global.READ {
		lock.ReadQueue = append(lock.ReadQueue, requestorAddress)
	} else {
		lock.WriteQueue = append(lock.WriteQueue, requestorAddress)
	}
	canProceed := false
	hasPrintedLog := false
	for !canProceed {
		switch requestType {
		case global.READ:
			canProceed = lock.WriteCount == 0 && lock.ReadCount < 2 && (len(lock.WriteQueue) == 0 || lock.ConsecutiveReads < 4) && lock.ReadQueue[0] == requestorAddress
		case global.WRITE:
			canProceed = lock.WriteCount == 0 && lock.ReadCount == 0 && (len(lock.ReadQueue) == 0 || lock.ConsecutiveWrites < 4) && lock.WriteQueue[0] == requestorAddress
		}
		if !canProceed {
			if !hasPrintedLog {
				fmt.Printf("Waiting for lock for file %s\n", fileName)
				hasPrintedLog = true
			}
			lock.FileLocksMutex.Unlock()
			// Optionally, sleep for a short time before trying again
			// time.Sleep(time.Millisecond * 50)
			lock.FileLocksMutex.Lock()
		}
	}
	// Grant lock
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

func releaseLock(fileName string, requestType global.RequestType) {
	global.GlobalFileLock.Lock()
	lock, exists := global.FileLocks[fileName]
	global.GlobalFileLock.Unlock()
	if !exists {
		return
	}
	lock.FileLocksMutex.Lock()
	defer lock.FileLocksMutex.Unlock()
	if requestType == global.READ {
		fmt.Printf("Released read lock for file %s\n", fileName)
		lock.ReadCount--
		lock.ReadQueue = lock.ReadQueue[1:]
	} else {
		fmt.Printf("Released write lock for file %s\n", fileName)
		lock.WriteCount--
		lock.WriteQueue = lock.WriteQueue[1:]
	}
}

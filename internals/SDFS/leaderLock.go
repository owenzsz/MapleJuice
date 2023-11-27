package SDFS

import (
	fd "cs425-mp/internals/failureDetector"
	"cs425-mp/internals/global"
	"fmt"
)

const (
	NUM_CONCURRENT_READ_LIMIT = 2
)

func cleanUpDeadNodesInLeaderLock() {
	global.GlobalFileLock.Lock()
	defer global.GlobalFileLock.Unlock()
	deadNodes := make([]string, 0)
	for _, node := range global.SERVER_ADDRS {
		if !fd.IsNodeAlive(node) {
			deadNodes = append(deadNodes, node)
		}
	}
	for _, deadNode := range deadNodes {
		for fileName, lock := range global.FileLocks {
			lock.FileLocksMutex.Lock()
			if global.Contains(lock.ReadQueue, deadNode) {
				if (len(lock.ReadQueue) > 0 && lock.ReadQueue[0] == deadNode) || (len(lock.ReadQueue) > 1 && lock.ReadQueue[1] == deadNode) {
					fmt.Printf("Released read lock for file %s due to dead node\n", fileName)
					lock.ReadCount--
				}
				newReadQueue, err := global.RemoveElementWithRange(lock.ReadQueue, deadNode, 0, len(lock.ReadQueue)-1)
				if err != nil {
					fmt.Printf("Error dequeing read queue: %s\n", err.Error())
				} else {
					lock.ReadQueue = newReadQueue
				}
			}
			if global.Contains(lock.WriteQueue, deadNode) {
				if lock.WriteQueue[0] == deadNode {
					lock.WriteCount--
					fmt.Printf("Released write lock for file %s due to dead node\n", fileName)
				}
				newWriteQueue, err := global.RemoveElementWithRange(lock.WriteQueue, deadNode, 0, len(lock.WriteQueue)-1)
				if err != nil {
					fmt.Printf("Error dequeing write queue: %s\n", err.Error())
				} else {
					lock.WriteQueue = newWriteQueue
				}
			}

			lock.FileLocksMutex.Unlock()
		}
	}
}

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
		newReadQueue, err := global.RemoveElementWithRange(lock.ReadQueue, requesterAddress, 0, NUM_CONCURRENT_READ_LIMIT-1)
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
		newWriteQueue, err := global.RemoveElementWithRange(lock.ReadQueue, requesterAddress, 0, 0)
		if err != nil {
			fmt.Printf("Error dequeing read queue: %s\n", err.Error())
		} else {
			lock.WriteQueue = newWriteQueue
		}
	}
}

package SDFS

import (
	"crypto/md5"
	fd "cs425-mp/internals/failureDetector"
	"cs425-mp/internals/global"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Empty struct{}

// update mem tables
func (mt *MemTable) delete(sdfsFileName string) {
	for _, files := range mt.VMToFileMap {
		delete(files, sdfsFileName)
	}
	delete(mt.fileToVMMap, sdfsFileName)
}

func (mt *MemTable) put(sdfsFileName string, replicas []string) {
	if _, exists := mt.fileToVMMap[sdfsFileName]; !exists {
		mt.fileToVMMap[sdfsFileName] = make(map[string]Empty)
	}
	for _, r := range replicas {
		if _, exists := mt.VMToFileMap[r]; !exists {
			mt.VMToFileMap[r] = make(map[string]Empty)
		}
		mt.VMToFileMap[r][sdfsFileName] = Empty{}
		mt.fileToVMMap[sdfsFileName][r] = Empty{}
	}
}

func hashFileName(fileName string) string {
	hash := md5.Sum([]byte(fileName))
	hashString := fmt.Sprintf("%v", hash)
	total := 0
	for i := 0; i < 10; i++ {
		total += int(hashString[i])
	}
	// add one because our VM ids start from 1
	return fmt.Sprintf("%v", total%10+1)
}

func isCurrentNodeLeader() bool {
	return HOSTNAME == LEADER_ADDRESS
}

func getDefaultReplicaVMAddresses(id string) []string {
	membershipList := fd.GetAllNodeAddresses()
	replicaSize := global.Min(4, len(membershipList))

	replicas := make([]string, 0)

	if replicaSize < NUM_WRITE {
		for i := 0; i < replicaSize; i++ {
			if fd.IsNodeAlive(membershipList[i]) {
				replicas = append(replicas, membershipList[i])
			}
		}
	} else {
		val, err := strconv.Atoi(id)
		if err != nil {
			fmt.Println("Input id cannot be parsed to int")
		}
		i := 0

		for len(replicas) < replicaSize {
			hostName := getFullHostNameFromID(fmt.Sprintf("%v", ((val+i)%10 + 1)))
			if fd.IsNodeAlive(hostName) {
				replicas = append(replicas, hostName)
			}
			i++
		}
	}
	return replicas
}

func getFullHostNameFromID(id string) string {
	numID, err := strconv.Atoi(id) // Convert string to integer
	if err != nil {
		fmt.Println("getFullHostNameFromID: Invalid input ID")
		return ""
	}
	return fmt.Sprintf("fa23-cs425-18%02d.cs.illinois.edu", numID)
}

func getScpHostNameFromHostName(hostName string) string {
	id := getIDFromFullHostName(hostName)
	return fmt.Sprintf("cs425-%s", id)
}

func getIDFromFullHostName(hostName string) string { // might use it later if we decide to do the decode/encode locally
	components := strings.Split(hostName, "-")
	if len(components) < 3 {
		fmt.Printf("Get ID from host name with invalid name: %v \n", hostName)
		return ""
	}
	idWithSuffix := components[2]
	idOnly := strings.Split(idWithSuffix, ".")[0]

	id := idOnly[2:]
	if len(id) > 1 && strings.HasPrefix(id, "0") {
		return id[1:]
	}
	return id
}

func getAllSDFSFilesForVM(vmAddress string) []string {
	var fileNames []string
	files, exists := memTable.VMToFileMap[vmAddress]
	if !exists {
		return fileNames
	}
	for fileName := range files {
		fileNames = append(fileNames, fileName)
	}
	return fileNames
}

func listSDFSFileVMs(sdfsFileName string) []string {
	var VMList []string
	val, exists := memTable.fileToVMMap[sdfsFileName]
	if !exists {
		fmt.Println("Error: file not exist")
	} else {
		for k := range val {
			VMList = append(VMList, k)
		}
	}
	return VMList
}

// Get local server ID based on hostname. Each machines should have a unique ID defined in global.go
// Returns -1 if not defined
func getLocalServerID() int {

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Error getting host name: ", err)
		return -1
	}
	for i, addr := range global.SERVER_ADDRS {
		if hostname == addr {
			return i + 1
		}
	}
	return -1
}

// Returns server's hostname given its ID
// Returns "" empty string if ID is not defined
func getServerName(id int) string {
	if id < 1 || id > len(global.SERVER_ADDRS) {
		return ""
	}
	return global.SERVER_ADDRS[id-1]
}


// Generate random duration bewteen 4-6 seconds
func randomDuration() time.Duration {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(3) + 4
	return time.Duration(n) * time.Second
}

func getAlivePeersAddrs() []string {
	localServerAddr := getServerName(getLocalServerID())
	fd.NodeListLock.Lock()
	addrList := []string{}
	for _, node := range fd.NodeInfoList {
		nodeName := node.NodeAddr
		if (nodeName != localServerAddr) && (node.Status == fd.Alive || node.Status == fd.Suspected) {
			addrList = append(addrList, nodeName)
		}
	}
	fd.NodeListLock.Unlock()

	return addrList
}

// find elements in a that are not in b
func findDisjointElements(A, B []string) []string {
	// Create a map to store elements of array B
	bMap := make(map[string]bool)
	for _, elem := range B {
		bMap[elem] = true
	}

	// Find elements in A that are not in B
	var disjoint []string
	for _, elem := range A {
		if _, exists := bMap[elem]; !exists {
			disjoint = append(disjoint, elem)
		}
	}

	return disjoint
}

func deleteAllFiles(dir string) error {
	// Open the directory.
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	// Read directory entries.
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}

	// Iterate over the directory entries and delete each file.
	for _, name := range names {
		filePath := filepath.Join(dir, name)
		err = os.Remove(filePath)
		if err != nil {
			return err
		}
	}
	return nil
}

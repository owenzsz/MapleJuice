package global

import (
	pb "cs425-mp/protobuf"
	"fmt"
	"strconv"
	"sync"
)

/************************************************* Leader's States ***************************************************************/
type Empty struct{}
type _MemTable struct {
	FileToVMMap      map[string]map[string]Empty
	VMToFileMap      map[string]map[string]Empty
	FileLineCountMap map[string]int
}

type RequestType int

const (
	READ RequestType = iota
	WRITE
)

type FileLock struct {
	ReadQueue         []string
	WriteQueue        []string
	ReadCount         int
	WriteCount        int
	ConsecutiveReads  int
	ConsecutiveWrites int
	FileLocksMutex    sync.Mutex
}

var (
	FileLocks      = make(map[string]*FileLock)
	GlobalFileLock = sync.Mutex{}
	MemtableLock   = sync.Mutex{}
)

var (
	MemTable = &_MemTable{
		FileToVMMap:      make(map[string]map[string]Empty), // go does not have sets, so we used a map with empty value to repersent set
		VMToFileMap:      make(map[string]map[string]Empty),
		FileLineCountMap: make(map[string]int),
	}
	Version = 0 // Monotonically incremented when leader send Gossip in Failure Detector Module
)

// update mem tables
func (mt *_MemTable) DeleteFile(sdfsFileName string) {
	MemtableLock.Lock()
	for _, files := range mt.VMToFileMap {
		delete(files, sdfsFileName)
	}
	delete(mt.FileToVMMap, sdfsFileName)
	delete(mt.FileLineCountMap, sdfsFileName)
	MemtableLock.Unlock()
}

func (mt *_MemTable) DeleteVM(VMAddr string) {
	MemtableLock.Lock()
	for _, VMs := range mt.FileToVMMap {
		delete(VMs, VMAddr)
	}
	delete(mt.VMToFileMap, VMAddr)
	MemtableLock.Unlock()
}

func (mt *_MemTable) Put(sdfsFileName string, replicas []string, lineCount int) {
	MemtableLock.Lock()
	if _, exists := mt.FileToVMMap[sdfsFileName]; !exists {
		mt.FileToVMMap[sdfsFileName] = make(map[string]Empty)
	}
	for _, r := range replicas {
		if _, exists := mt.VMToFileMap[r]; !exists {
			mt.VMToFileMap[r] = make(map[string]Empty)
		}
		mt.VMToFileMap[r][sdfsFileName] = Empty{}
		mt.FileToVMMap[sdfsFileName][r] = Empty{}
	}
	mt.FileLineCountMap[sdfsFileName] = lineCount
	MemtableLock.Unlock()
}

// Serialize leader state to protobuf format.
// myAddr is the hostname of the calling node of this function
func LeaderStatesToPB(myAddr string) *pb.LeaderState {
	res := &pb.LeaderState{}
	// Include memtable
	MemtableLock.Lock()
	res.FileToVMMap = make(map[string]*pb.LeaderState_AddrList)
	for key, value := range MemTable.FileToVMMap {
		res.FileToVMMap[key] = &pb.LeaderState_AddrList{}
		res.FileToVMMap[key].VMAddr = make([]string, 0)
		// vm_list := vm_addr_list.VMAddr
		for vm_addr := range value {
			res.FileToVMMap[key].VMAddr = append(res.FileToVMMap[key].VMAddr, vm_addr)
		}
	}

	res.VMToFileMap = make(map[string]*pb.LeaderState_FileList)
	for key, value := range MemTable.VMToFileMap {
		res.VMToFileMap[key] = &pb.LeaderState_FileList{}
		res.VMToFileMap[key].FileNames = make([]string, 0)
		// file_name_list := file_list.FileNames
		for file_name := range value {
			res.VMToFileMap[key].FileNames = append(res.VMToFileMap[key].FileNames, file_name)
		}
	}

	res.FileLineCountMap = make(map[string]int64)
	for key, value := range MemTable.FileLineCountMap {
		res.FileLineCountMap[key] = int64(value)
	}
	MemtableLock.Unlock()

	// Include file lock
	GlobalFileLock.Lock()
	res.FileLocks = make(map[string]*pb.LeaderState_FileLock)
	for filename, filelock := range FileLocks {
		res.FileLocks[filename] = &pb.LeaderState_FileLock{}
		res.FileLocks[filename].ReadQueue = make([]string, 0)
		res.FileLocks[filename].WriteQueue = make([]string, 0)
		res.FileLocks[filename].ReadQueue = append(res.FileLocks[filename].ReadQueue, filelock.ReadQueue...)
		res.FileLocks[filename].WriteQueue = append(res.FileLocks[filename].WriteQueue, filelock.WriteQueue...)
		res.FileLocks[filename].ReadCount = int32(filelock.ReadCount)
		res.FileLocks[filename].WriteCount = int32(filelock.WriteCount)
		res.FileLocks[filename].ConsecutiveReads = int32(filelock.ConsecutiveReads)
		res.FileLocks[filename].ConsecutiveWrites = int32(filelock.ConsecutiveWrites)
	}
	GlobalFileLock.Unlock()

	// If Leader node, increase Memtable Version number by one
	if myAddr == GetLeaderAddress() {
		Version++
	}
	res.Version = int64(Version)

	// fmt.Printf("Leader State is = %v\n", MemTable.FileToVMMap)
	// fmt.Printf("Serialized to = %v\n", res.FileToVMMap)
	// for k, v := range MemTable.FileToVMMap {
	// 	if len(res.FileToVMMap[k].VMAddr) != len(v) {
	// 		panic("1. Not equal length")
	// 	}
	// }
	// for k, v := range MemTable.VMToFileMap {
	// 	if len(res.VMToFileMap[k].FileNames) != len(v) {
	// 		panic("2. Not equal length")
	// 	}
	// }
	// fmt.Println()

	return res
}

func UpdateLeaderStateIfNecessary(leaderStates *pb.LeaderState) {
	if leaderStates.Version <= int64(Version) {
		return
	}
	Version = int(leaderStates.Version)

	MemtableLock.Lock()
	new_file_to_VM_map := make(map[string]map[string]Empty)
	for k, v := range leaderStates.FileToVMMap {
		addr_list := v.VMAddr
		new_file_to_VM_map[k] = make(map[string]Empty)
		for _, addr := range addr_list {
			new_file_to_VM_map[k][addr] = Empty{}
		}
	}
	MemTable.FileToVMMap = new_file_to_VM_map

	new_VM_to_file_map := make(map[string]map[string]Empty)
	for k, v := range leaderStates.VMToFileMap {
		file_list := v.FileNames
		new_VM_to_file_map[k] = make(map[string]Empty)
		for _, filename := range file_list {
			new_VM_to_file_map[k][filename] = Empty{}
		}
	}
	MemTable.VMToFileMap = new_VM_to_file_map

	new_file_line_count_map := make(map[string]int)
	for k, v := range leaderStates.FileLineCountMap {
		new_file_line_count_map[k] = int(v)
	}
	MemTable.FileLineCountMap = new_file_line_count_map
	MemtableLock.Unlock()

	GlobalFileLock.Lock()
	FileLocks = make(map[string]*FileLock)
	for filename, v := range leaderStates.FileLocks {
		FileLocks[filename] = &FileLock{}
		if v.ReadQueue != nil {
			FileLocks[filename].ReadQueue = make([]string, 0)
			FileLocks[filename].ReadQueue = v.ReadQueue
		}
		if v.WriteQueue != nil {
			FileLocks[filename].WriteQueue = make([]string, 0)
			FileLocks[filename].WriteQueue = v.WriteQueue
		}
		FileLocks[filename].ReadCount = int(v.ReadCount)
		FileLocks[filename].WriteCount = int(v.WriteCount)
		FileLocks[filename].ConsecutiveReads = int(v.ConsecutiveReads)
		FileLocks[filename].ConsecutiveWrites = int(v.ConsecutiveWrites)
	}
	GlobalFileLock.Unlock()

}

/************************************************* Leader Information ***************************************************************/
var (
	LeaderID = -1 // Record which machine is the leader for now. -1 is the default value and mean there is no leader, at least this machine thinks
)

func GetLeaderID() int {
	return int(LeaderID)
}

func GetLeaderAddress() string {
	id := GetLeaderID()
	var format_parameter string
	if id == 10 {
		format_parameter = strconv.Itoa(id)
	} else {
		format_parameter = "0" + strconv.Itoa(id)
	}
	return fmt.Sprintf("fa23-cs425-18%v.cs.illinois.edu", format_parameter)
}

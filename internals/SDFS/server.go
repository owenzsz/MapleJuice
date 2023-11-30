package SDFS

import (
	"context"
	fd "cs425-mp/internals/failureDetector"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	NUM_WRITE = 4
	NUM_READ  = 1
)

var (
	SDFS_PATH string
	HOSTNAME  string
)

// Record file append's latest version. Used for idempotent file append
var appendVersionMap map[string]int = make(map[string]int)

func init() {
	usr, err := user.Current()
	if err != nil {
		fmt.Printf("Error getting user home directory: %v \n", err)
	}
	SDFS_PATH = filepath.Join(usr.HomeDir, "SDFS_Files")
	deleteAllFiles(SDFS_PATH)
	hn, err := os.Hostname()
	if err != nil {
		fmt.Printf("Error getting hostname: %v \n", err)
		panic(err)
	}
	HOSTNAME = hn
}

func PeriodicLeaderTasks() {
	for {
		if IsCurrentNodeLeader() {
			cleanMemtableAndReplicate()
		}
		time.Sleep(5 * time.Second)
	}
}

func cleanMemtableAndReplicate() {
	global.MemtableLock.Lock()
	VMsToCleanUp := make([]string, 0)
	for VM := range global.MemTable.VMToFileMap {
		if !fd.IsNodeAlive(VM) {
			VMsToCleanUp = append(VMsToCleanUp, VM)
		}
	}
	global.MemtableLock.Unlock()
	for _, VM := range VMsToCleanUp {
		global.MemTable.DeleteVM(VM) //memtable is locked in the DeleteVM method
	}

	go cleanUpDeadNodesInLeaderLock()

	replicationStartTime := time.Now()
	needToReplicate := false
	replicationTasks := make(map[string][]string) // map from fileName to list of receiverAddresses

	global.MemtableLock.Lock()
	for fileName := range global.MemTable.FileToVMMap {
		replicas := listSDFSFileVMs(fileName)
		if len(replicas) < NUM_WRITE {
			needToReplicate = true
			allAliveNodes := GetAlivePeersAddrs()
			disjointAddresses := findDisjointElements(allAliveNodes, replicas)
			fmt.Printf("Randomly selecting %v neighbors\n", NUM_WRITE-len(replicas))
			receiverAddresses, err := randomSelect(disjointAddresses, NUM_WRITE-len(replicas))
			if err != nil {
				fmt.Printf("Error selecting random addresses: %v\n", err)
			} else {
				replicationTasks[fileName] = receiverAddresses
			}
		}
	}
	global.MemtableLock.Unlock() // Unlock as quickly as possible

	for fileName, receiverAddresses := range replicationTasks {
		replicas := listSDFSFileVMs(fileName) // list again to get the most updated replicas
		if len(replicas) == 0 {
			fmt.Printf("No replicas found for file %s. Skipping replication.\n", fileName)
			continue
		}
		senderAddress := replicas[0]
		r := sendReplicateFileRequest(senderAddress, receiverAddresses, fileName)
		if r == nil || !r.Success {
			//TODO: add logic for failed replication
			fmt.Printf("Failed to replicate file %s from %s to %+q\n", fileName, senderAddress, receiverAddresses)
		} else {
			fmt.Printf("Successfully replicated file %s from %s to %+q\n", fileName, senderAddress, receiverAddresses)
		}
	}

	if needToReplicate {
		replicationOperationTime := time.Since(replicationStartTime).Milliseconds()
		fmt.Printf("Replication time: %d ms\n", replicationOperationTime)
	}

}

func sendReplicateFileRequest(senderMachine string, receiverMachines []string, fileName string) *pb.ReplicationResponse {
	conn, err := grpc.Dial(senderMachine+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	defer conn.Close()

	c := pb.NewSDFSClient(conn)

	r, err := c.ReplicateFile(context.Background(), &pb.ReplicationRequest{
		FileName:         fileName,
		SenderMachine:    senderMachine,
		ReceiverMachines: receiverMachines,
	})
	if err != nil {
		fmt.Printf("Failed to call replicate: %v\n", err)
	}
	return r
}

// SDFS Server
type SDFSServer struct {
	pb.UnimplementedSDFSServer
}

// Get file
func (s *SDFSServer) GetFile(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	fileName := in.FileName
	var vmList []string
	var canProceed bool
	if global.MP_NUMBER != 4 {
		canProceed = requestLock(in.RequesterAddress, fileName, global.READ)
	} else {
		canProceed = true
	}
	if canProceed {
		vmList = listSDFSFileVMs(in.FileName)
	}
	resp := &pb.GetResponse{
		Success:     true,
		VMAddresses: vmList,
		ShouldWait:  !canProceed,
	}
	return resp, nil
}

// Get ACK (sent to leader)
func (s *SDFSServer) GetACK(ctx context.Context, in *pb.GetACKRequest) (*pb.GetACKResponse, error) {
	if global.MP_NUMBER != 4 {
		releaseLock(in.RequesterAddress, in.FileName, global.READ)
	}
	resp := &pb.GetACKResponse{
		Success: true,
	}
	return resp, nil
}

func (s *SDFSServer) MultiGetFile(ctx context.Context, in *pb.MultiGetRequest) (*pb.MultiGetResponse, error) {
	HandleGetFile(in.SdfsFileName, in.LocalFileName)
	resp := &pb.MultiGetResponse{
		Success: true,
	}
	return resp, nil
}

// Put file
func (s *SDFSServer) PutFile(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	fileName := in.FileName
	canProceed := requestLock(in.RequesterAddress, fileName, global.WRITE)
	var targetReplicas []string
	if canProceed {
		val, exists := global.MemTable.FileToVMMap[fileName]
		if !exists {
			targetReplicas = getDefaultReplicaVMAddresses(hashFileName(fileName))
		} else {
			for k := range val {
				targetReplicas = append(targetReplicas, k)
			}
		}
	}

	resp := &pb.PutResponse{
		Success:     true,
		VMAddresses: targetReplicas,
		ShouldWait:  !canProceed,
	}
	return resp, nil
}

// put ACK (sent to leader)
func (s *SDFSServer) PutACK(ctx context.Context, in *pb.PutACKRequest) (*pb.PutACKResponse, error) {
	fileName := in.FileName
	vmAddress := in.ReplicaAddresses
	lineCount := in.LineCount
	//update file table
	if in.IsAppend {
		originalLinCount := global.MemTable.FileLineCountMap[fileName]
		lineCount += int64(originalLinCount)
	}
	global.MemTable.Put(fileName, vmAddress, int(lineCount))
	if !in.IsReplicate {
		releaseLock(in.RequesterAddress, fileName, global.WRITE)
	}
	resp := &pb.PutACKResponse{
		Success: true,
	}
	return resp, nil
}

// append
func (s *SDFSServer) AppendNewContent(ctx context.Context, in *pb.AppendNewContentRequest) (*pb.AppendNewContentResponse, error) {
	// Ignore duplicate append, append should be idempotent
	if currVersion, ok := appendVersionMap[in.FileName]; ok && currVersion == int(in.Version) {
		return &pb.AppendNewContentResponse{
			Success: true,
		}, nil
	}
	fPath := filepath.Join(SDFS_PATH, in.FileName)
	// If the file doesn't exist, create it, or append to the file
	f, err := os.OpenFile(fPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// Cannot find the file to be appended
		fmt.Printf("cannot open file %s to append: %v\n", fPath, err)
		return &pb.AppendNewContentResponse{
			Success: false,
		}, nil
	}
	if _, err := f.Write([]byte(in.Content)); err != nil {
		// Cannot actually append the new content to the file
		fmt.Printf("cannot append to file %s: %v\n", fPath, err)
		return &pb.AppendNewContentResponse{
			Success: false,
		}, nil
	}
	if err := f.Close(); err != nil {
		// Cannot close the appended file
		fmt.Printf("cannot close the appended file %s: %v\n", fPath, err)
		return &pb.AppendNewContentResponse{
			Success: false,
		}, nil
	}
	return &pb.AppendNewContentResponse{
		Success: true,
	}, nil
}

func (s *SDFSServer) MultiPutFile(ctx context.Context, in *pb.MultiPutRequest) (*pb.MultiPutResponse, error) {
	HandlePutFile(in.LocalFileName, in.SdfsFileName)
	resp := &pb.MultiPutResponse{
		Success: true,
	}
	return resp, nil
}

// Delete file (leader)
func (s *SDFSServer) DeleteFileLeader(ctx context.Context, in *pb.DeleteRequestLeader) (*pb.DeleteResponseLeader, error) {
	fileName := in.FileName
	// vmList := listSDFSFileVMs(fileName)
	// err := sendDeleteFileMessageToTargetFollowers(vmList, fileName)
	// resp := &pb.DeleteResponseLeader{}
	// if err != nil {
	// 	resp.Success = false
	// } else {
	// 	resp.Success = true
	// 	memTable.delete(fileName)
	// }
	resp := &pb.DeleteResponseLeader{
		Success: true,
	}
	global.MemTable.DeleteFile(fileName)
	return resp, nil
}

// func sendDeleteFileMessageToTargetFollowers(targetFollowers []string, fileName string) error {
// 	var err error
// 	errCh := make(chan error, len(targetFollowers))
// 	for _, f := range targetFollowers {
// 		go func(f string) {
// 			err := sendDeleteFileRequestsToFollower(f, fileName)
// 			errCh <- err
// 		}(f)
// 	}

// 	for i := 0; i < len(targetFollowers); i++ {
// 		err = <-errCh
// 		if err != nil {
// 			fmt.Printf("Error deleting file on follower: %v\n", err)
// 		}
// 	}
// 	close(errCh)
// 	return err
// }

// func sendDeleteFileRequestsToFollower(targetFollower string, fileName string) error {
// 	conn, err := grpc.Dial(targetFollower+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		fmt.Printf("did not connect: %v\n", err)
// 	}
// 	defer conn.Close()

// 	c := pb.NewSDFSClient(conn)

// 	r, err := c.DeleteFileFollower(context.Background(), &pb.DeleteRequestFollower{
// 		FileName: fileName,
// 	})

// 	if err != nil {
// 		fmt.Printf("Failed to call delete on followers: %v\n", err)
// 	}
// 	if !r.Success {
// 		return fmt.Errorf("failed to delete file %s on follower %s", fileName, targetFollower)
// 	}
// 	return nil
// }

// Delete file (follower)
func (s *SDFSServer) DeleteFileFollower(ctx context.Context, in *pb.DeleteRequestFollower) (*pb.DeleteResponseFollower, error) {
	fileName := in.FileName
	err := deleteLocalSDFSFile(fileName)
	resp := &pb.DeleteResponseFollower{}
	if err != nil {
		fmt.Printf("Failed to delete local file : %v\n", err)
		resp.Success = false
	} else {
		resp.Success = true
	}
	return resp, nil
}

func deleteLocalSDFSFile(sdfsFileName string) error {
	files, err := os.ReadDir(SDFS_PATH)
	if err != nil {
		return err
	}
	for _, file := range files {
		filePath := filepath.Join(SDFS_PATH, file.Name())
		if !file.IsDir() && strings.HasPrefix(file.Name(), sdfsFileName) {
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// list local files (store)
func (s *SDFSServer) ListLocalFiles(ctx context.Context, in *pb.ListLocalFilesRequest) (*pb.ListLocalFilesResponse, error) {
	requestorHostName := in.SenderAddress
	fileNameList := getAllSDFSFilesForVM(requestorHostName)
	resp := &pb.ListLocalFilesResponse{
		Success:   true,
		FileNames: fileNameList,
	}
	return resp, nil
}

// List file holders (LS)
func (s *SDFSServer) ListFileHolder(ctx context.Context, in *pb.ListFileHolderRequest) (*pb.ListFileHolderResponse, error) {
	fileName := in.FileName
	vmList := listSDFSFileVMs(fileName)
	lineCount := global.MemTable.FileLineCountMap[fileName]
	resp := &pb.ListFileHolderResponse{
		Success:     true,
		VMAddresses: vmList,
		LineCount:   int64(lineCount),
	}
	return resp, nil
}

// replicate file upon detecting failures
func (s *SDFSServer) ReplicateFile(ctx context.Context, in *pb.ReplicationRequest) (*pb.ReplicationResponse, error) {
	resp := &pb.ReplicationResponse{
		Success: true,
	}
	if in.SenderMachine != HOSTNAME {
		fmt.Println("Error: received replication request for a different machine")
		resp.Success = false
	}
	fmt.Println("Replicating file")
	localSDFSFilePath := filepath.Join(SDFS_PATH, in.FileName)
	err := transferFilesConcurrent(localSDFSFilePath, in.FileName, in.ReceiverMachines)
	if err != nil {
		fmt.Printf("Failed to transfer file: %v\n", err)
		resp.Success = false
	}
	lineCount, err := global.CountFileLines(localSDFSFilePath)
	sendPutACKToLeader(lineCount, in.FileName, in.ReceiverMachines, true, false)
	if err != nil {
		fmt.Printf("Failed to count line number: %v\n", err)
		resp.Success = false
	}
	return resp, err
}

func StartSDFSServer() {
	lis, err := net.Listen("tcp", ":"+global.SDFS_PORT)
	if err != nil {
		fmt.Printf("failed to listen: %v\n", err)
	}
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024 * 1024 * 1024),
	)
	pb.RegisterSDFSServer(s, &SDFSServer{})
	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v\n", err)
	}
}

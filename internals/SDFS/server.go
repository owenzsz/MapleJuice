package SDFS

import (
	"context"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// LEADER_ADDRESS = "fa23-cs425-1801.cs.illinois.edu" // Default leader's receiving address
	NUM_WRITE      = 4
	NUM_READ       = 1
)

var (
	SDFS_PATH string
	// memTable  = &MemTable{
	// 	fileToVMMap: make(map[string]map[string]Empty), // go does not have sets, so we used a map with empty value to repersent set
	// 	VMToFileMap: make(map[string]map[string]Empty),
	// }
	HOSTNAME   string
	FD_CHANNEL chan string // channel to communicate with FD, same as the fd.SDFS_CHANNEL
)

// type MemTable struct {
// 	fileToVMMap map[string]map[string]Empty
// 	VMToFileMap map[string]map[string]Empty
// }

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

func SetFDChannel(ch chan string) {
	FD_CHANNEL = ch
}

func ObserveFDChannel() {
	for {
		msg := <-FD_CHANNEL
		fmt.Printf("Received message from FD: %s\n", msg)
		components := strings.Split(msg, ":")
		messageType := components[0]
		nodeAddr := components[1]
		if messageType == "Failed" {
			go handleNodeFailure(nodeAddr)
		}
	}
}

// handle failure of a node
func handleNodeFailure(failedNodeAddr string) {
	if !isCurrentNodeLeader() {
		return
	}
	fmt.Println("Handling node failure")
	//find out all the files that the failed node has
	filesToReplicate := global.MemTable.VMToFileMap[failedNodeAddr]
	//for each file, get a list of alived machines that contain the file
	for fileName := range filesToReplicate {
		replicas := listSDFSFileVMs(fileName)
		senderAddress := replicas[0]
		allAliveNodes := getAlivePeersAddrs()
		disjointAddresses := findDisjointElements(allAliveNodes, replicas)
		// randomly select an alive machine to replicate the file
		receiverAddress := disjointAddresses[rand.Intn(len(disjointAddresses))]
		r := sendReplicateFileRequest(senderAddress, receiverAddress, fileName)
		if r == nil || !r.Success {
			//TODO: add logic for failed replication
			fmt.Printf("Failed to replicate file %s from %s to %s\n", fileName, senderAddress, receiverAddress)
		} else {
			fmt.Printf("Successfully replicated file %s from %s to %s\n", fileName, senderAddress, receiverAddress)
		}
	}
	//remove the VM from the mem table
	delete(global.MemTable.VMToFileMap, failedNodeAddr)
}

func sendReplicateFileRequest(senderMachine string, receiverMachine string, fileName string) *pb.ReplicationResponse {
	conn, err := grpc.Dial(senderMachine+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	defer conn.Close()

	c := pb.NewSDFSClient(conn)

	r, err := c.ReplicateFile(context.Background(), &pb.ReplicationRequest{
		FileName:        fileName,
		SenderMachine:   senderMachine,
		ReceiverMachine: receiverMachine,
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
	requestLock(in.RequesterAddress, fileName, global.READ)
	vmList := listSDFSFileVMs(in.FileName)
	resp := &pb.GetResponse{
		Success:     true,
		VMAddresses: vmList,
	}
	return resp, nil
}

// Get ACK (sent to leader)
func (s *SDFSServer) GetACK(ctx context.Context, in *pb.GetACKRequest) (*pb.GetACKResponse, error) {
	releaseLock(in.FileName, global.READ)
	resp := &pb.GetACKResponse{
		Success: true,
	}
	return resp, nil
}

// Put file
func (s *SDFSServer) PutFile(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	fileName := in.FileName
	requestLock(in.RequesterAddress, fileName, global.WRITE)
	var targetReplicas []string
	val, exists := global.MemTable.FileToVMMap[fileName]
	if !exists {
		targetReplicas = getDefaultReplicaVMAddresses(hashFileName(fileName))
	} else {
		for k := range val {
			targetReplicas = append(targetReplicas, k)
		}
	}
	resp := &pb.PutResponse{
		Success:     true,
		VMAddresses: targetReplicas,
	}
	return resp, nil
}

// put ACK (sent to leader)
func (s *SDFSServer) PutACK(ctx context.Context, in *pb.PutACKRequest) (*pb.PutACKResponse, error) {
	fileName := in.FileName
	vmAddress := in.ReplicaAddresses
	//update file table
	global.MemTable.Put(fileName, vmAddress)
	releaseLock(fileName, global.WRITE)
	resp := &pb.PutACKResponse{
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
	global.MemTable.Delete(fileName)
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
	resp := &pb.ListFileHolderResponse{
		Success:     true,
		VMAddresses: vmList,
	}
	return resp, nil
}

// replicate file upon detecting failures
func (s *SDFSServer) ReplicateFile(ctx context.Context, in *pb.ReplicationRequest) (*pb.ReplicationResponse, error) {
	resp := &pb.ReplicationResponse{}
	if in.SenderMachine != HOSTNAME {
		fmt.Println("Error: received replication request for a different machine")
		resp.Success = false
	}
	fmt.Println("Replicating file")
	localSDFSFilePath := filepath.Join(SDFS_PATH, in.FileName)
	err := transferFile(localSDFSFilePath, in.FileName, []string{in.ReceiverMachine})
	if err != nil {
		fmt.Printf("Failed to transfer file: %v\n", err)
		resp.Success = false
	} else {
		resp.Success = true
	}
	return resp, err
}

func StartSDFSServer() {
	lis, err := net.Listen("tcp", ":"+global.SDFS_PORT)
	if err != nil {
		fmt.Printf("failed to listen: %v\n", err)
	}
	s := grpc.NewServer()
	pb.RegisterSDFSServer(s, &SDFSServer{})
	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v\n", err)
	}
}

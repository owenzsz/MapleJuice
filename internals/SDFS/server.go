package SDFS

import (
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/proto"
)

const (
	LEADER_ADDRESS = "fa23-cs425-1801.cs.illinois.edu" // Default leader's receiving address
	NUM_WRITE      = 4
	NUM_READ       = 1
)

var (
	SDFS_PATH string
	memTable  = &MemTable{
		fileToVMMap: make(map[string]map[string]Empty), // go does not have sets, so we used a map with empty value to repersent set
		VMToFileMap: make(map[string]map[string]Empty),
	}
	HOSTNAME   string
	FD_CHANNEL chan string // channel to communicate with FD, same as the fd.SDFS_CHANNEL
)

type MemTable struct {
	fileToVMMap map[string]map[string]Empty
	VMToFileMap map[string]map[string]Empty
}

func init() {
	usr, err := user.Current()
	if err != nil {
		fmt.Printf("Error getting user home directory: %v \n", err)
	}
	SDFS_PATH = filepath.Join(usr.HomeDir, "SDFS_Files")
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

func handleNodeFailure(failedNodeAddr string) {
	//find out all the files that the failed node has
	filesToReplicate := memTable.VMToFileMap[failedNodeAddr]
	//for each file, get a list of alived machines that contain the file
	for fileName := range filesToReplicate {
		replicas := listSDFSFileVMs(fileName)
		replicaSize := len(replicas)
		// randomly select a machine to replicate the file
		// if the current machine is selected, replicate the file
		for replicaSize < NUM_WRITE {
			//randomly select an alive machine, send message to it to ask it to replicate the file
		}
	}
	//remove the VM from the mem table
	delete(memTable.VMToFileMap, failedNodeAddr)
}

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

// handle incoming SDFS messages
func HandleSDFSMessages() {
	listener, err := net.Listen("tcp", ":"+global.SDFS_PORT)
	if err != nil {
		fmt.Println("Error listening to TCP connections: ", err)
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting: %v\n", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 4096)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Error reading: %v\n", err.Error())
			return
		}
		request := &pb.SDFSRequest{}
		err = proto.Unmarshal(buffer[:n], request)
		if err != nil {
			fmt.Printf("Error unmarshalling SDFS message: %v\n", err.Error())
			return
		}
		switch request.RequestType {
		case pb.SDFSRequestType_GET_REQ:
			processGetMessage(request, conn)
		case pb.SDFSRequestType_PUT_REQ:
			processPutMessage(request, conn)
		case pb.SDFSRequestType_DELETE_REQ_LEADER:
			processDeleteMessageLeader(request, conn)
		case pb.SDFSRequestType_DELETE_REQ_FOLLOWER:
			processDeleteMessageFollower(request, conn)
		case pb.SDFSRequestType_LS_REQ:
			processLSMessage(request, conn)
		case pb.SDFSRequestType_STORE_REQ:
			processStoreMessage(request, conn)
		}

	}
}

func processGetMessage(message *pb.SDFSRequest, conn net.Conn) {
	fileName := message.SdfsFileName
	vmList := listSDFSFileVMs(fileName)
	response := &pb.SDFSResponse{
		ResponseType: pb.SDFSResponseType_GET_RES,
		VMAddresses:  vmList,
	}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		fmt.Printf("Failed to marshal Get Response: %v\n", err.Error())
	}
	conn.Write(responseBytes)
}

func processPutMessage(message *pb.SDFSRequest, conn net.Conn) {
	fileName := message.SdfsFileName
	var targetReplicas []string
	val, exists := memTable.fileToVMMap[fileName]
	if !exists {
		targetReplicas = getDefaultReplicaVMAddresses(hashFileName(fileName))
		memTable.put(fileName, targetReplicas)
	} else {
		for k := range val {
			targetReplicas = append(targetReplicas, k)
		}
	}
	response := &pb.SDFSResponse{
		ResponseType: pb.SDFSResponseType_GET_RES,
		VMAddresses:  targetReplicas,
	}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		fmt.Printf("Failed to marshal Put Response: %v\n", err.Error())
	}
	_, err = conn.Write(responseBytes)
	if err != nil {
		fmt.Println("Error writing to TCP: ", err)
		return
	}
}

func processDeleteMessageLeader(message *pb.SDFSRequest, conn net.Conn) {
	if !isCurrentNodeLeader() {
		fmt.Println("Not leader, cannot process delete message")
		return
	}

	fileName := message.SdfsFileName
	vmList := listSDFSFileVMs(fileName)
	response := sendDeleteFileMessageToReplicaNodes(fileName, vmList)
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		fmt.Printf("Failed to marshal Delete Response: %v\n", err.Error())
	}
	conn.Write(responseBytes)
	memTable.delete(fileName)
}

func processDeleteMessageFollower(message *pb.SDFSRequest, conn net.Conn) {
	fileName := message.SdfsFileName
	err := deleteLocalSDFSFile(fileName)
	response := &pb.SDFSResponse{
		ResponseType: pb.SDFSResponseType_DELETE_RES_LEADER,
	}
	if err != nil {
		fmt.Printf("Failed to delete local file : %v\n", err)
		response.Error = err.Error()
		response.ResponseStatus = pb.SDFSResponseStatus_RES_STATUS_FAILED
	} else {
		response.ResponseStatus = pb.SDFSResponseStatus_RES_STATUS_OK
	}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		fmt.Printf("Failed to marshal Delete Response: %v\n", err.Error())
	}
	conn.Write(responseBytes)
}

func processLSMessage(message *pb.SDFSRequest, conn net.Conn) {
	fileName := message.SdfsFileName
	vmList := listSDFSFileVMs(fileName)
	response := &pb.SDFSResponse{
		ResponseType: pb.SDFSResponseType_LS_RES,
		VMAddresses:  vmList,
	}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		fmt.Printf("Failed to marshal GetResponse: %v\n", err.Error())
	}
	conn.Write(responseBytes)
}

func processStoreMessage(message *pb.SDFSRequest, conn net.Conn) {
	requestorHostName := message.VM
	fileNameList := getAllSDFSFilesForVM(requestorHostName)
	response := &pb.SDFSResponse{
		ResponseType:  pb.SDFSResponseType_LS_RES,
		SdfsFileNames: fileNameList,
	}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		fmt.Printf("Failed to marshal GetResponse: %v\n", err.Error())
	}
	conn.Write(responseBytes)
}

// SDFS file operations
func getFile(sdfsFileName string, localFileName string) {
	res := sendGetFileMessage(sdfsFileName)
	replicas := res.VMAddresses
	for _, r := range replicas {
		remotePath := getScpHostNameFromHostName(r) + ":" + filepath.Join(SDFS_PATH, sdfsFileName)
		cmd := exec.Command("scp", remotePath, localFileName)
		err := cmd.Start()
		if err != nil {
			fmt.Printf("Failed to start command: %v\n", err)
			return
		}

		err = cmd.Wait()
		if err != nil {
			fmt.Printf("Command finished with error: %v\n", err)
			continue
		}
		break
	}

}
func putFile(localFileName string, sdfsFileName string) {
	if _, err := os.Stat(localFileName); os.IsNotExist(err) {
		fmt.Printf("Local file not exist: %s\n", localFileName)
		return
	}
	res := sendPutFileMessage(sdfsFileName)
	targetReplicas := res.VMAddresses
	fmt.Printf("Put file %s to sdfs %s \n", localFileName, sdfsFileName)
	for _, r := range targetReplicas {
		targetHostName := getScpHostNameFromHostName(r)
		remotePath := targetHostName + ":" + filepath.Join(SDFS_PATH, sdfsFileName)
		cmd := exec.Command("scp", localFileName, remotePath)
		err := cmd.Start()
		if err != nil {
			fmt.Printf("Failed to start command: %v\n", err)
			return
		}

		err = cmd.Wait()
		if err != nil {
			fmt.Printf("Command finished with error: %v\n", err)
			return
		}
	}
	fmt.Printf("Put file to replicas: %+q\n", targetReplicas)
}

func deleteFile(sdfsFileName string) {
	err := deleteLocalSDFSFile(sdfsFileName)
	if err != nil {
		fmt.Printf("Failed to delete local file : %v\n", err)
		return
	}
	res := sendDeleteFileMessageToLeader(sdfsFileName)
	if res.ResponseStatus == pb.SDFSResponseStatus_RES_STATUS_OK {
		fmt.Printf("Successfully deleted file %s\n", sdfsFileName)
	} else {
		fmt.Printf("Failed to delete file %s: %s\n", sdfsFileName, res.Error)
	}
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

func LS(sdfsFileName string) {
	res := sendLSMessage(sdfsFileName)
	VMList := res.VMAddresses
	fmt.Printf("%+q\n", VMList)
}

func store() {
	res := sendStoreMessage(HOSTNAME)
	fileNameList := res.SdfsFileNames
	fmt.Printf("%+q\n", fileNameList)
}

// Send messages to leader server
func sendMesageToLeader(messageBytes []byte) *pb.SDFSResponse {
	return sendMesageToNode(LEADER_ADDRESS, messageBytes)
}

func sendMesageToNode(nodeAddr string, messageBytes []byte) *pb.SDFSResponse {
	conn, err := net.Dial("tcp", nodeAddr+":"+global.SDFS_PORT)
	if err != nil {
		fmt.Println("Error dialing TCP: ", err)
		return nil
	}
	defer conn.Close()
	_, err = conn.Write(messageBytes)
	if err != nil {
		fmt.Println("Error sending TCP: ", err)
		return nil
	}
	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading from TCP: ", err)
	}
	res := &pb.SDFSResponse{}
	err = proto.Unmarshal(buffer[:n], res)
	if err != nil {
		fmt.Println("Deserialization error: ", err)
	}
	return res
}

func sendGetFileMessage(fileName string) *pb.SDFSResponse {
	putMessage := &pb.SDFSRequest{
		RequestType:  pb.SDFSRequestType_GET_REQ,
		SdfsFileName: fileName,
	}
	messageBytes, err := proto.Marshal(putMessage)
	if err != nil {
		fmt.Printf("Failed to marshal PutMessage: %v\n", err.Error())
	}
	res := sendMesageToLeader(messageBytes)
	return res
}

func sendPutFileMessage(fileName string) *pb.SDFSResponse {
	putMessage := &pb.SDFSRequest{
		RequestType:  pb.SDFSRequestType_PUT_REQ,
		SdfsFileName: fileName,
	}
	messageBytes, err := proto.Marshal(putMessage)
	if err != nil {
		fmt.Printf("Failed to marshal PutMessage: %v\n", err.Error())
	}
	res := sendMesageToLeader(messageBytes)
	return res
}

func sendDeleteFileMessageToLeader(fileName string) *pb.SDFSResponse {
	deleteMessage := &pb.SDFSRequest{
		RequestType:  pb.SDFSRequestType_DELETE_REQ_LEADER,
		SdfsFileName: fileName,
	}
	messageBytes, err := proto.Marshal(deleteMessage)
	if err != nil {
		fmt.Printf("Failed to marshal DeleteMessage: %v\n", err.Error())
	}
	res := sendMesageToLeader(messageBytes)
	return res
}

func sendDeleteFileMessageToReplicaNodes(fileName string, replicas []string) *pb.SDFSResponse {
	deleteMessage := &pb.SDFSRequest{
		RequestType:  pb.SDFSRequestType_DELETE_REQ_FOLLOWER,
		SdfsFileName: fileName,
	}
	messageBytes, err := proto.Marshal(deleteMessage)
	if err != nil {
		fmt.Printf("Failed to marshal DeleteMessage: %v\n", err.Error())
	}
	response := &pb.SDFSResponse{
		ResponseType:   pb.SDFSResponseType_DELETE_RES_FOLLOWER,
		ResponseStatus: pb.SDFSResponseStatus_RES_STATUS_OK,
	}
	for _, r := range replicas {
		res := sendMesageToNode(r, messageBytes)
		if res.ResponseStatus == pb.SDFSResponseStatus_RES_STATUS_FAILED {
			response.Error = res.Error
			response.ResponseStatus = pb.SDFSResponseStatus_RES_STATUS_FAILED
			continue
		}
	}
	return response
}

func sendLSMessage(fileName string) *pb.SDFSResponse {
	lsMessage := &pb.SDFSRequest{
		RequestType:  pb.SDFSRequestType_LS_REQ,
		SdfsFileName: fileName,
	}
	messageBytes, err := proto.Marshal(lsMessage)
	if err != nil {
		fmt.Printf("Failed to marshal lsMessage: %v\n", err.Error())
	}
	res := sendMesageToLeader(messageBytes)
	return res
}

func sendStoreMessage(vmName string) *pb.SDFSResponse {
	storeMessage := &pb.SDFSRequest{
		RequestType: pb.SDFSRequestType_STORE_REQ,
		VM:          vmName,
	}
	messageBytes, err := proto.Marshal(storeMessage)
	if err != nil {
		fmt.Printf("Failed to marshal StoreMessage: %v\n", err.Error())
	}
	res := sendMesageToLeader(messageBytes)
	return res
}

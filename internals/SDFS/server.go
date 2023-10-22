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
	HOSTNAME string
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
		case pb.SDFSRequestType_DELETE_REQ:
			processDeleteMessage(request, conn)
		case pb.SDFSRequestType_LS_REQ:
			processLSMessage(request, conn)
		case pb.SDFSRequestType_STORE_REQ:
			processStoreMessage(request, conn)
		}

	}
}

func processGetMessage(message *pb.SDFSRequest, conn net.Conn) {
	fmt.Println("Received Get Message")
	fileName := message.SdfsFileName
	vmList := listSDFSFileVMs(fileName)
	response := &pb.SDFSResponse{
		ResponseType: pb.SDFSResponseType_GET_RES,
		VMAddresses:  vmList,
	}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		fmt.Printf("Failed to marshal GetResponse: %v\n", err.Error())
	}
	conn.Write(responseBytes)
}

func processPutMessage(message *pb.SDFSRequest, conn net.Conn) {
	fmt.Println("Received Put Message")
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
		fmt.Printf("Failed to marshal GetResponse: %v\n", err.Error())
	}
	conn.Write(responseBytes)
}

func processDeleteMessage(message *pb.SDFSRequest, conn net.Conn) {
	fmt.Println("Received Delete Message")
	fileName := message.SdfsFileName
	// err := deleteLocalSDFSFile(fileName)
	// if err != nil {
	// 	fmt.Printf("Failed to delete local file : %v\n", err)
	// 	return
	// }
	memTable.delete(fileName)
	//TODO: finish the implementation of delete message
	//1. if the current server is the leader, send the list of VMs that have the current file
	//2. if the current server is not the leader, check if the file exists locally and delete it
}

func processLSMessage(message *pb.SDFSRequest, conn net.Conn) {
	fmt.Println("Received LS Message")
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
	fmt.Println("Received Store Message")
	requestorHostName := message.VM
	fileNameList := getAllLocalSDFSFilesForVM(requestorHostName)
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
		fmt.Printf("Remote path is: %s\n", remotePath)
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
	fmt.Printf("Put file to 4 replicas: %+q\n", targetReplicas)
}

func deleteFile(sdfsFileName string) {
	err := deleteLocalSDFSFile(sdfsFileName)
	if err != nil {
		fmt.Printf("Failed to delete local file : %v\n", err)
		return
	}
	memTable.delete(sdfsFileName)
	sendDeleteFileMessage(sdfsFileName)
}

func deleteLocalSDFSFile(sdfsFileName string) error {
	files, err := os.ReadDir(SDFS_PATH)
	if err != nil {
		return err
	}
	for _, file := range files {
		filePath := filepath.Join(SDFS_PATH, file.Name())
		if !file.IsDir() && strings.HasPrefix(file.Name(), sdfsFileName) {
			fmt.Printf("Try to delete file %s.\n", file.Name())
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

//

// Send messages to leader server
func sendMesageToLeader(messageBytes []byte) *pb.SDFSResponse {
	conn, err := net.Dial("tcp", LEADER_ADDRESS+":"+global.SDFS_PORT)
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

func sendDeleteFileMessage(fileName string) *pb.SDFSResponse {
	deleteMessage := &pb.SDFSRequest{
		RequestType:  pb.SDFSRequestType_DELETE_REQ,
		SdfsFileName: fileName,
	}
	messageBytes, err := proto.Marshal(deleteMessage)
	if err != nil {
		fmt.Printf("Failed to marshal DeleteMessage: %v\n", err.Error())
	}
	res := sendMesageToLeader(messageBytes)
	return res
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

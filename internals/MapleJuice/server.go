package maplejuice

import (
	"bufio"
	"context"
	sdfs "cs425-mp/internals/SDFS"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"

	"google.golang.org/grpc"
)

type MapleJuiceServer struct {
	pb.UnimplementedMapleJuiceServer
}

// var (
// 	MAPLE_INTERMEDIATE_FILES_FOLDER string
// 	JUICE_INTERMEDIATE_FILES_FOLDER string
// )

// func init() {
// 	usr, err := user.Current()
// 	if err != nil {
// 		fmt.Printf("Error getting user home directory: %v \n", err)
// 	}
// 	MAPLE_INTERMEDIATE_FILES_FOLDER = filepath.Join(usr.HomeDir, "MAPLE_INTERMEDIATE_FILES")

// 	err = os.MkdirAll(MAPLE_INTERMEDIATE_FILES_FOLDER, 0777)
// 	if err != nil {
// 		fmt.Println("Error creating Maple directory:", err)
// 		return
// 	}
// }

func (s *MapleJuiceServer) GetMapleWorkerList(ctx context.Context, in *pb.MapleWorkerListRequest) (*pb.MapleWorkerListeResponse, error) {
	if sdfs.IsCurrentNodeLeader() {
		workersCount := in.NumMaples
		dir := in.SdfsSrcDirectory
		workerAssignments := assignMapleWorkToWorkers(dir, int(workersCount))
		resp := &pb.MapleWorkerListeResponse{
			Success:     true,
			Assignments: workerAssignments,
		}
		return resp, nil
	} else {
		fmt.Println("Not the leader, cannot get maple worker list")
		return nil, fmt.Errorf("not the leader, cannot get maple worker list")
	}
}

func (s *MapleJuiceServer) Maple(ctx context.Context, in *pb.MapleRequest) (*pb.MapleResponse, error) {
	//1. get file from SDFS
	sdfs.HandleGetFile(in.MapleExePath, in.MapleExePath)
	for _, file := range in.Files {
		sdfs.HandleGetFile(file.Filename, file.Filename)
	}
	//2. run maple exe on the file and designated line
	//3. for each key, output to a temporary local file
	runExecutableFileOnInputFiles(in.MapleExePath, in.Files, in.SdfsIntermediateFilenamePrefix)
	//4. append the temporary local file to intermediate file on SDFS
	resp := &pb.MapleResponse{
		Success: true,
	}
	return resp, nil
}

func runExecutableFileOnInputFiles(mapleExePath string, fileLines []*pb.FileLines, prefix string) error {
	var wg sync.WaitGroup
	var mapleExeErrors []error
	var mut sync.Mutex
	for _, fileLine := range fileLines {
		wg.Add(1)
		go func(fileLine *pb.FileLines) {
			defer wg.Done()
			KVCollection, err := runExecutableFileOnSingleInputFile(mapleExePath, fileLine)
			if err != nil {
				mut.Lock()
				mapleExeErrors = append(mapleExeErrors, err)
				mut.Unlock()
			} else {
				err = appendAllIntermediateResultToSDFS(KVCollection, prefix)
				if err != nil {
					mut.Lock()
					mapleExeErrors = append(mapleExeErrors, err)
					mut.Unlock()
				}
			}
		}(fileLine)
	}
	wg.Wait()
	if len(mapleExeErrors) > 0 {
		return fmt.Errorf("some maple execution tasks failed: %v", mapleExeErrors)
	}

	fmt.Printf("Successfully finished excuting maple exe\n")
	return nil
}

func runExecutableFileOnSingleInputFile(mapleExePath string, fileLine *pb.FileLines) (map[string][]string, error) {
	KVCollection := make(map[string][]string)
	file := fileLine.Filename
	startLine := int(fileLine.Range.Start)
	endLine := int(fileLine.Range.End)
	currentLine := 0
	inputFile, err := os.Open(file)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}
	defer inputFile.Close()
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		if currentLine >= startLine && currentLine <= endLine {
			line := scanner.Text()
			cmd := exec.Command("python3", mapleExePath)
			cmd.Stdin = strings.NewReader(line)
			output, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Printf("Error executing script on line %d: %s\n", currentLine, err)
				continue
			}
			kvPairs := strings.Split(string(output), "\n")
			for _, kvPair := range kvPairs {
				kv := strings.Split(kvPair, ":")
				if len(kv) != 2 {
					continue
				}
				key := kv[0]
				value := kv[1]
				KVCollection[key] = append(KVCollection[key], value)
			}
			fmt.Printf("Output from line %d: %s\n", currentLine, string(output))
		}
		currentLine++
		if currentLine > endLine {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading from input file:", err)
		return nil, err
	}
	// err = writeKVToFile(KVCollection)
	// if err != nil {
	// 	fmt.Println("Error writing to KV Collection to files:", err)
	// 	return err
	// }

	return KVCollection, nil
}

// func writeKVToFile(KVCollection map[string][]string) error {
// 	for key, values := range KVCollection {
// 		fileName := key
// 		fPath := filepath.Join(MAPLE_INTERMEDIATE_FILES_FOLDER, fileName)
// 		// Open the file if it exists, or create it if it doesn't
// 		file, err := os.OpenFile(fPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 		if err != nil {
// 			fmt.Printf("Error opening or creating file for key %s: %s\n", key, err)
// 			return err
// 		}
// 		defer file.Close()

// 		for _, value := range values {
// 			_, err := file.WriteString(fmt.Sprintf("{%s:%s}\n", key, value))
// 			if err != nil {
// 				fmt.Printf("Error writing to file for key %s: %s\n", key, err)
// 				return err
// 			}
// 		}
// 	}
// 	return nil
// }

func appendAllIntermediateResultToSDFS(KVCollection map[string][]string, prefix string) error {
	// Iterate over the directory entries and delete each file.
	for key, values := range KVCollection {
		var content string
		for _, v := range values {
			content += fmt.Sprintf("%s:%s\n", key, v)
		}
		sdfsIntermediateFileName := fmt.Sprintf("%s_%s", prefix, key)
		fmt.Printf("Trying to appended to SDFS file %s\n", sdfsIntermediateFileName)
		sdfs.HandleAppendFile(sdfsIntermediateFileName, content)
	}

	return nil
}

func StartMapleJuiceServer() {
	// start listening for incoming connections
	lis, err := net.Listen("tcp", ":"+global.MAPLE_JUICE_PORT)
	if err != nil {
		fmt.Printf("failed to listen: %v\n", err)
	}
	s := grpc.NewServer()
	pb.RegisterMapleJuiceServer(s, &MapleJuiceServer{})
	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v\n", err)
	}
}

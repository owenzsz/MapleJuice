package maplejuice

import (
	"context"
	sdfs "cs425-mp/internals/SDFS"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//maple juice

func handleMaple(mapleExePath string, numMaples int, intermediateFileNamepPrefix string, sourceDirectory string) {
	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	leaderConn, err := grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.MAPLE_JUICE_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	c := pb.NewMapleJuiceClient(leaderConn)
	resp, err := c.Maple(context.Background(), &pb.MapleRequest{
		NumMaples:                      int32(numMaples),
		MapleExePath:                   mapleExePath,
		SdfsSrcDirectory:               sourceDirectory,
		SdfsIntermediateFilenamePrefix: intermediateFileNamepPrefix,
	})
	if err != nil || !resp.Success {
		fmt.Printf("Maple failed: %v\n", err)
	} else {
		fmt.Printf("Successfully finished processing maple request\n")
	}
}

func sendMapleRequestToWorkers(assignments map[string][]*pb.FileLines, mapleExePath string, intermediateFileNamePrefix string) error {
	occupiedVM := make(map[string]global.Empty)
	for k := range assignments {
		occupiedVM[k] = global.Empty{}
	}

	var wg sync.WaitGroup
	var mapleErrors []error
	var mut sync.Mutex

	for workerAddr, assignment := range assignments {
		wg.Add(1)
		go func(_worrkerAddr string, _assignment []*pb.FileLines, _occupiedVM map[string]global.Empty) {
			defer wg.Done()
			err := sendMapleRequestToSingleWorker(_worrkerAddr, _assignment, mapleExePath, intermediateFileNamePrefix, occupiedVM)
			if err != nil {
				mut.Lock()
				mapleErrors = append(mapleErrors, err)
				mut.Unlock()
			}
		}(workerAddr, assignment, occupiedVM)
	}

	wg.Wait()

	if len(mapleErrors) > 0 {
		return fmt.Errorf("some maple tasks failed: %v", mapleErrors)
	}

	fmt.Printf("Successfully finished excuting maple command \n")
	return nil
}

func sendMapleRequestToSingleWorker(workerAddr string, assignment []*pb.FileLines, mapleExePath string, intermediateFileNamePrefix string, occupiedVM map[string]global.Empty) error {
	var err error
	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.DialContext(ctx, workerAddr+":"+global.MAPLE_JUICE_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer dialCancel()
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
		alivePeers := sdfs.GetAlivePeersAddrs()
		for _, vmAddr := range alivePeers {
			if vmAddr == global.GetLeaderAddress() {
				continue
			}
			if _, ok := occupiedVM[vmAddr]; !ok {
				// Choose this new vm and schedule the inputFiles to this vm
				occupiedVM[vmAddr] = global.Empty{}
				// With heuristic that this recursion would end eventually, we take a leap of faith
				fmt.Printf("Rescheduling maple request while dialing to %v\n", vmAddr)
				return sendMapleRequestToSingleWorker(vmAddr, assignment, mapleExePath, intermediateFileNamePrefix, occupiedVM)
			}
		}
		return err
	}
	defer conn.Close()
	c := pb.NewMapleJuiceClient(conn)

	resp, err := c.MapleExec(context.Background(), &pb.MapleExecRequest{
		MapleExePath:                   mapleExePath,
		SdfsIntermediateFilenamePrefix: intermediateFileNamePrefix,
		Files:                          assignment,
	})
	if err != nil {
		alivePeers := sdfs.GetAlivePeersAddrs()
		for _, vmAddr := range alivePeers {
			// We make the assumption leader will not do any Maple/Juice tasks
			if vmAddr == global.GetLeaderAddress() {
				continue
			}
			if _, ok := occupiedVM[vmAddr]; !ok {
				// Choose this new vm and schedule the inputFiles to this vm
				occupiedVM[vmAddr] = global.Empty{}
				// With heuristic that this recursion would end eventually, we take a leap of faith
				fmt.Printf("Rescheduling maple request while sending exec to %v\n", vmAddr)
				return sendMapleRequestToSingleWorker(vmAddr, assignment, mapleExePath, intermediateFileNamePrefix, occupiedVM)
			}
		}
		return err
	}
	if resp == nil || !resp.Success {
		return fmt.Errorf("node %v processed maple request unsuccessfully: %v", workerAddr, err)
	}
	fmt.Printf("Node %v processed maple request successfully\n", workerAddr)
	return nil
}

func handleJuice(juiceExePath string, numJuicer int, intermediateFileNamePrefix string, destFileName string, deleteInput bool, isRangePartition bool) {
	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	leaderConn, err := grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.MAPLE_JUICE_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	c := pb.NewMapleJuiceClient(leaderConn)

	// Add request max response time limit
	// timeout := 3 * time.Second
	// ctx, callCancel := context.WithTimeout(context.Background(), timeout)
	// defer callCancel()

	_, err = c.Juice(context.Background(), &pb.JuiceRequest{
		NumJuicer:        int32(numJuicer),
		DeleteInput:      deleteInput,
		IsRangePartition: isRangePartition,
		JuiceExecName:    juiceExePath,
		Prefix:           intermediateFileNamePrefix,
		DestName:         destFileName,
	})

	if err != nil {
		fmt.Printf("Juice failed: %v\n", err)
	} else {
		fmt.Printf("Successfully finished excuting juice command \n")
	}
}

// Dispatch juice tasks to VMs and deal with rescheduling if worker fails
func dispatchJuiceTasksToVMs(keyAssignment map[string]map[string]global.Empty, juiceProgram string, dstFileName string) error {
	occupiedVM := make(map[string]global.Empty)
	for k := range keyAssignment {
		occupiedVM[k] = global.Empty{}
	}

	var wg sync.WaitGroup
	errs := make([]error, len(keyAssignment))
	errExists := false

	i := 0
	for vm, inputFiles := range keyAssignment {
		wg.Add(1)
		go func(_vm string, _inputFiles map[string]global.Empty, i int, _occupiedVM map[string]global.Empty) {
			defer wg.Done()
			err := dispatchJuiceTaskToSingleVM(_vm, _inputFiles, juiceProgram, dstFileName, _occupiedVM)
			if err != nil {
				errs[i] = err
				errExists = true
			}
		}(vm, inputFiles, i, occupiedVM)
		i++
	}

	wg.Wait()
	if errExists {
		return errors.New("JuiceTask failed")
	}
	return nil

}

func dispatchJuiceTaskToSingleVM(vm string, inputFiles map[string]global.Empty, juiceProgram string, dstFileName string, occupiedVM map[string]global.Empty) error {
	ctx, dialCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(ctx, vm+":"+global.MAPLE_JUICE_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
		// Rescheduling current works to a new VM
		alivePeers := sdfs.GetAlivePeersAddrs()
		for _, vmAddr := range alivePeers {
			// We make the assumption leader will not do any Maple/Juice tasks
			if vmAddr == global.GetLeaderAddress() {
				continue
			}
			if _, ok := occupiedVM[vmAddr]; !ok {
				// Choose this new vm and schedule the inputFiles to this vm
				occupiedVM[vmAddr] = global.Empty{}
				// With heuristic that this recursion would end eventually, we take a leap of faith
				return dispatchJuiceTaskToSingleVM(vmAddr, inputFiles, juiceProgram, dstFileName, occupiedVM)
			}
		}
		return err
	}
	c := pb.NewMapleJuiceClient(conn)
	inputFilesList := make([]string, 0)
	for k := range inputFiles {
		inputFilesList = append(inputFilesList, k)
	}
	resp, err := c.JuiceExec(context.Background(), &pb.JuiceExecRequest{
		JuiceProgram:     juiceProgram,
		DstFilename:      dstFileName,
		InputIntermFiles: inputFilesList,
	})
	if err != nil {
		fmt.Printf("juice exec failed due to external errors: %v\n", err)
		// todo: Rescheduling or Retry?? Current choice - Rescheduling
		// Rescheduling current works to a new VM
		alivePeers := sdfs.GetAlivePeersAddrs()
		for _, vmAddr := range alivePeers {
			// We make the assumption leader will not do any Maple/Juice tasks
			if vmAddr == global.GetLeaderAddress() {
				continue
			}
			if _, ok := occupiedVM[vmAddr]; !ok {
				// Choose this new vm and schedule the inputFiles to this vm
				occupiedVM[vmAddr] = global.Empty{}
				// With heuristic that this recursion would end eventually, we take a leap of faith
				return dispatchJuiceTaskToSingleVM(vmAddr, inputFiles, juiceProgram, dstFileName, occupiedVM)
			}
		}
		return err
	}
	if !resp.Success {
		fmt.Printf("juice exec failed due to logic errors: resp.Success = false\n")
		return errors.New("juice exec failed due to logic errors: resp.Success = false")
	}
	return nil
}

func handleSQL(query string) {
	dataset, field, regex, err := matchFilterPattern(query)
	if err != nil {
		dataset1, dataset2, field1, field2, err := matchJoinPattern(query)
		if err != nil {
			fmt.Printf("Invalid SQL query format\n")
			return
		}
		handleSQLJoin(dataset1, dataset2, field1, field2)
		return
	}
	handleSQLFilter(dataset, field, regex)
}

func matchFilterPattern(query string) (string, string, string, error) {
	pattern := `SELECT ALL FROM (\w+) WHERE (\w+) REGEX (.+)`
	re := regexp.MustCompile(pattern)

	// Match the pattern in the given sqlQuery
	matches := re.FindStringSubmatch(query)
	if len(matches) < 3 {
		return "", "", "", fmt.Errorf("invalid filter query format")
	}

	// Extracted groups: Dataset, Regex Condition
	return matches[1], matches[2], matches[3], nil
}

func matchJoinPattern(query string) (string, string, string, string, error) {
	// SELECT ALL FROM D1, D2 WHERE D1.name = D2.ID
	pattern := `SELECT ALL FROM (\w+), (\w+) WHERE (\w+\.\w+) = (\w+\.\w+)`
	re := regexp.MustCompile(pattern)

	// Match the pattern in the given sqlQuery
	matches := re.FindStringSubmatch(query)
	if len(matches) < 5 {
		return "", "", "", "", fmt.Errorf("invalid join query format")
	}
	return matches[1], matches[2], matches[3], matches[4], nil
}

func handleSQLFilter(dataset string, field string, regex string) {
	schemaFileName := dataset + "_schema"
	sdfs.HandleGetFile(schemaFileName, schemaFileName)
	schema, err := extractSchemaFromSchemaFile(schemaFileName)
	if err != nil {
		fmt.Printf("Error extracting schema from file: %v\n", err)
		return
	}
	mapleExeFileName, err := generateFilterMapleExeFileWithRegex(regex, schema, field)
	if err != nil {
		fmt.Printf("Error generating maple filter exe file: %v\n", err)
	}
	sdfs.HandlePutFile(mapleExeFileName, mapleExeFileName)
	handleMaple(mapleExeFileName, 5, "filter", dataset)

	juiceExeFileName, err := generateJuiceFilterExeFile()
	if err != nil {
		fmt.Printf("Error generating juice filter exe file: %v\n", err)
	}
	sdfs.HandlePutFile(juiceExeFileName, juiceExeFileName)
	handleJuice(juiceExeFileName, 5, "filter", dataset+"_filtered", true, true)
	sdfs.HandleGetFile(dataset+"_filtered", dataset+"_filtered")
}

func handleSQLJoin(dataset1 string, dataset2 string, condition1 string, condition2 string) {
	// handleMaple("SQL_join_map.py", 5, "join", dataset1)
	// handleMaple("SQL_join_map.py", 5, "join", dataset2)
	// handleJuice("SQL_join_reduce.py", 5, "join", dataset2+"_joined", true, true)
}

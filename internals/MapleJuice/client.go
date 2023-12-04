package maplejuice

import (
	"context"
	sdfs "cs425-mp/internals/SDFS"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
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

	mjCustomLog(true, "Successfully finished excuting maple command \n")
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
		dataset1, dataset2, leftDataset, col1, rightDataset, col2, err := matchJoinPattern(query)
		if err != nil {
			fmt.Printf("Invalid SQL query format\n")
			return
		}
		fmt.Printf("::::::::: %v, %v, %v, %v, %v, %v\n", dataset1, dataset2, leftDataset, col1, rightDataset, col2)
		directoryName := longestCommonPrefix([]string{dataset1, dataset2})
		if len(directoryName) == 0 {
			fmt.Printf("Cannot join two tables not in the same sdfs directory (sharing some prefix)")
			return
		}
		// Check if the order of tables specified in the query is correct
		if dataset1 == leftDataset && dataset2 == rightDataset {
			handleSQLJoin(dataset1, col1, dataset2, col2, directoryName)
		} else if dataset1 == rightDataset && dataset2 == leftDataset {
			handleSQLJoin(dataset2, col1, dataset1, col2, directoryName)
		} else {
			fmt.Printf("Invalid SQL query format, check tables")
		}
		return
	}
	handleSQLFilter(dataset, field, regex)
}

func matchFilterPattern(query string) (string, string, string, error) {
	patternREG := `SELECT ALL FROM (\w+) WHERE (\w+) REGEXP (.+)`
	patternLine := `SELECT ALL FROM (\w+) WHERE (.+)`
	re := regexp.MustCompile(patternREG)

	// Match the pattern in the given sqlQuery
	matches := re.FindStringSubmatch(query)
	if len(matches) < 3 {
		re = regexp.MustCompile(patternLine)
		matches = re.FindStringSubmatch(query)
		if len(matches) < 2 {
			return "", "", "", fmt.Errorf("invalid filter query format")
		}
		return matches[1], "", matches[2], nil
	}

	// Extracted groups: Dataset, Regex Condition
	return matches[1], matches[2], matches[3], nil
}

func matchJoinPattern(query string) (string, string, string, string, string, string, error) {
	// SELECT ALL FROM D1, D2 WHERE D1.name = D2.ID
	pattern := `SELECT ALL FROM (\w+), (\w+) WHERE (\w+)\.(\w+) = (\w+)\.(\w+)`
	re := regexp.MustCompile(pattern)

	// Match the pattern in the given sqlQuery
	matches := re.FindStringSubmatch(query)
	if len(matches) < 5 {
		return "", "", "", "", "", "", fmt.Errorf("invalid join query format")
	}
	return matches[1], matches[2], matches[3], matches[4], matches[5], matches[6], nil
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
	filterStartTime := time.Now()
	sdfs.HandlePutFile(mapleExeFileName, mapleExeFileName)
	mapleStartTime := time.Now()

	//generate a random string between 1 and 10000
	intermediatePrefix := "filter" + strconv.Itoa(rand.Intn(10000))

	handleMaple(mapleExeFileName, 4, intermediatePrefix, dataset)
	mapleExecutionTime := time.Since(mapleStartTime).Milliseconds()
	mjCustomLog(true, "Maple execution time for filter: %vms\n", mapleExecutionTime)
	juiceExeFileName, err := generateJuiceFilterExeFile()
	if err != nil {
		fmt.Printf("Error generating juice filter exe file: %v\n", err)
	}
	sdfs.HandlePutFile(juiceExeFileName, juiceExeFileName)
	juiceStartTime := time.Now()

	//generate a random string between 1 and 10000
	resultFileName := "res_" + strconv.Itoa(rand.Intn(10000))
	handleJuice(juiceExeFileName, 4, intermediatePrefix, resultFileName, true, true)
	juiceExecutionTime := time.Since(juiceStartTime).Milliseconds()
	mjCustomLog(true, "Juice execution time for filter: %vms\n", juiceExecutionTime)
	sdfs.HandleGetFile(resultFileName, dataset+"_filtered")
	filterExecutionTime := time.Since(filterStartTime).Milliseconds()
	mjCustomLog(true, "Filter execution time: %vms\n", filterExecutionTime)
}

func handleSQLJoin(table1 string, column1 string, table2 string, column2 string, directoryName string) {
	// Get schema of the first table
	schema1FileName := table1 + "_schema"
	sdfs.HandleGetFile(schema1FileName, schema1FileName)
	schema1, err := extractSchemaFromSchemaFile(schema1FileName)
	if err != nil {
		fmt.Printf("Error extracting schema from file: %v\n", err)
		return
	}
	// Get schema of the second table
	schema2FileName := table2 + "_schema"
	sdfs.HandleGetFile(schema2FileName, schema2FileName)
	schema2, err := extractSchemaFromSchemaFile(schema2FileName)
	if err != nil {
		fmt.Printf("Error extracting schema from file: %v\n", err)
		return
	}
	// Generate maple exe that will be used by the SQL JOIN operation
	MapleExeFileName, err := generateJoinMapleExeFile(table1, column1, schema1, table2, column2, schema2)
	if err != nil {
		fmt.Printf("Error generating maple join exe file: %v\n", err)
	}
	joinStartTime := time.Now()
	sdfs.HandlePutFile(MapleExeFileName, MapleExeFileName)
	mapleStartTime := time.Now()
	handleMaple(MapleExeFileName, 4, "join", directoryName)
	mapleExecutionTime := time.Since(mapleStartTime).Milliseconds()
	fmt.Printf("Maple execution time for join: %vms\n", mapleExecutionTime)

	JuiceExeFileName, err := generateJoinJuiceExeFile()
	if err != nil {
		fmt.Printf("Error generating juice filter exe file: %v\n", err)
	}
	sdfs.HandlePutFile(JuiceExeFileName, JuiceExeFileName)
	juiceStartTime := time.Now()
	handleJuice(JuiceExeFileName, 4, "join", directoryName+"_joined", true, true)
	juiceExecutionTime := time.Since(juiceStartTime).Milliseconds()
	fmt.Printf("Juice execution time for join: %vms\n", juiceExecutionTime)
	sdfs.HandleGetFile(directoryName+"_joined", directoryName+"_joined")
	// If post processing of the joined file is needed, add at below
	joinExecutionTime := time.Since(joinStartTime).Milliseconds()
	fmt.Printf("Join execution time: %vms\n", joinExecutionTime)
}

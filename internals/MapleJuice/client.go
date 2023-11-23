package maplejuice

import (
	"context"
	sdfs "cs425-mp/internals/SDFS"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//maple juice

func handleMaple(mapleExePath string, numMaples int, intermediateFileNamepPrefix string, sourceDirectory string) {
	// 1. get maple worker list
	workersAssignments, err := requestMapleTaskAssignments(numMaples, sourceDirectory)
	if err != nil {
		fmt.Printf("Failed to get maple worker list: %v\n", err)
	}
	// 2. send maple request to each worker
	err = sendMapleRequestToWorkers(workersAssignments, mapleExePath, intermediateFileNamepPrefix, sourceDirectory)
	if err != nil {
		fmt.Printf("Failed to execute maple task on workers: %v\n", err)
	}
	// 3. wait for all workers to finish
}

func requestMapleTaskAssignments(numMaples int, sourceDirectory string) ([]*pb.MapleWorkerListeResponse_WorkerTaskAssignment, error) {
	var conn *grpc.ClientConn
	var c pb.MapleJuiceClient
	var err error
	for {
		if conn == nil {
			ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer dialCancel()
			conn, err = grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.MAPLE_JUICE_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("did not connect: %v\n", err)
				continue
			}
			c = pb.NewMapleJuiceClient(conn)
		}

		// Add request max response time limit
		timeout := 3 * time.Second
		ctx, callCancel := context.WithTimeout(context.Background(), timeout)
		defer callCancel()

		resp, err := c.GetMapleWorkerList(ctx, &pb.MapleWorkerListRequest{
			NumMaples:        int32(numMaples),
			SdfsSrcDirectory: sourceDirectory,
		})
		if err != nil {
			fmt.Printf("Leader failed to process get maple worker list: %v\n", err)
			conn.Close()
			conn = nil
			time.Sleep(global.RETRY_CONN_SLEEP_TIME)
			fmt.Println("Retrying to send get maple worker list to leader")
			continue
		}
		if resp == nil || !resp.Success {
			fmt.Printf("Leader process get maple worker list unsuccessfully: %v\n", err)
			conn.Close()
			return nil, err
		}
		fmt.Printf("Leader processed get maple worker list successfully \n")
		conn.Close()
		return resp.Assignments, nil
	}
}

func sendMapleRequestToWorkers(assignments []*pb.MapleWorkerListeResponse_WorkerTaskAssignment, mapleExePath string, intermediateFileNamePrefix string, sourceDirectory string) error {
	var wg sync.WaitGroup
	var mapleErrors []error
	var mut sync.Mutex

	for _, ass := range assignments {
		wg.Add(1)
		go func(assignment *pb.MapleWorkerListeResponse_WorkerTaskAssignment) {
			defer wg.Done()
			err := sendMapleRequestToSingleWorker(assignment, mapleExePath, intermediateFileNamePrefix, sourceDirectory)
			if err != nil {
				mut.Lock()
				mapleErrors = append(mapleErrors, err)
				mut.Unlock()
			}
		}(ass)
	}

	wg.Wait()

	if len(mapleErrors) > 0 {
		return fmt.Errorf("some maple tasks failed: %v", mapleErrors)
	}

	fmt.Printf("Successfully finished excuting maple command \n")
	return nil
}

func sendMapleRequestToSingleWorker(assignment *pb.MapleWorkerListeResponse_WorkerTaskAssignment, mapleExePath string, intermediateFileNamePrefix string, sourceDirectory string) error {
	var err error
	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(ctx, assignment.WorkerAddress+":"+global.MAPLE_JUICE_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	defer conn.Close()
	c := pb.NewMapleJuiceClient(conn)

	// Add request max response time limit
	timeout := 3 * time.Second
	ctx, callCancel := context.WithTimeout(context.Background(), timeout)
	defer callCancel()

	resp, err := c.Maple(ctx, &pb.MapleRequest{
		MapleExePath:                   mapleExePath,
		SdfsIntermediateFilenamePrefix: intermediateFileNamePrefix,
		Files:                          assignment.Files,
	})
	if err != nil {
		return fmt.Errorf("node %v failed to process maple request: %v", assignment.WorkerAddress, err)
	}
	if resp == nil || !resp.Success {
		return fmt.Errorf("node %v processed maple request unsuccessfully: %v", assignment.WorkerAddress, err)
	}
	fmt.Printf("Node %v processed maple request successfully\n", assignment.WorkerAddress)
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

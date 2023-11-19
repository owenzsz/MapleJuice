package maplejuice

import (
	"context"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//maple juice

func handleMaple(mapleExePath string, numMaples int, intermediateFileNamepPrefix string, sourceDirectory string) {
	// 1. get maple worker list
	workersAssignments, err := requestWorkersAssignments(numMaples, sourceDirectory)
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

func requestWorkersAssignments(numMaples int, sourceDirectory string) ([]*pb.MapleWorkerListeResponse_WorkerTaskAssignment, error) {
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
	defer conn.Close()
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
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

func handleJuice(juiceExePath string, numJuices int, intermediateFileNamePrefix string, destFileName string, deleteInput bool, partitionType string) {

}

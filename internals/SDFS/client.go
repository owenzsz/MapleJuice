package SDFS

import (
	"bufio"
	"context"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SDFS file operations
func HandleGetFile(sdfsFileName string, localFileName string) {
	var conn *grpc.ClientConn
	var c pb.SDFSClient
	var err error
	readStartTime := time.Now()

	// Small optimization: get from local cache for the sdfs file copy if available
	usr, err := user.Current()
	if err != nil {
		fmt.Printf("Error getting user home directory: %v \n", err)
	}
	SDFS_PATH = filepath.Join(usr.HomeDir, "SDFS_Files")
	possibleCachePath := filepath.Join(SDFS_PATH, sdfsFileName)
	if _, err := os.Stat(possibleCachePath); err == nil {
		fmt.Printf("local cache exists\n")
		sourceFile := possibleCachePath
		destDir := filepath.Join(usr.HomeDir, "cs425-mp4")
		destinationFile := filepath.Join(destDir, localFileName)

		errList := make([]error, 0)
		source, err := os.Open(sourceFile) //open the source file
		if err != nil {
			fmt.Printf("copy cache failed\n")
			errList = append(errList, err)
		}
		defer source.Close()

		destination, err := os.Create(destinationFile) //create the destination file
		if err != nil {
			fmt.Printf("copy cache failed\n")
			errList = append(errList, err)
		}
		defer destination.Close()
		_, err = io.Copy(destination, source) //copy the contents of source to destination file
		if err != nil {
			fmt.Printf("copy cache failed\n")
			errList = append(errList, err)
		}
		// Premature return if local cache is found and there is no error copying it to current project directory
		if len(errList) == 0 {
			readOperationTime := time.Since(readStartTime).Milliseconds()
			fmt.Printf("Successfully get file %s in %v ms \n", sdfsFileName, readOperationTime)
			return
		}
	}

	for {
		if conn == nil {
			ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer dialCancel()
			conn, err = grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("did not connect: %v\n", err)
				continue
			}
			c = pb.NewSDFSClient(conn)
		}

		shouldWaitForLock := true
		resp := &pb.GetResponse{}
		for shouldWaitForLock {
			// Add request max response time limit
			timeout := 3 * time.Second
			ctx, callCancel := context.WithTimeout(context.Background(), timeout)
			defer callCancel()

			r, err := c.GetFile(ctx, &pb.GetRequest{
				RequesterAddress: HOSTNAME,
				FileName:         sdfsFileName,
			})

			if err != nil {
				fmt.Printf("Failed to call get: %v\n", err)
				conn.Close()
				conn = nil
				time.Sleep(global.RETRY_CONN_SLEEP_TIME)
				fmt.Printf("Retrying to get file %s\n", sdfsFileName)
				break
			}

			if r != nil {
				if !r.Success {
					fmt.Println("failed to acquire the list of vms to read the file from")
					conn.Close()
					return
				}

				if r.ShouldWait {
					fmt.Printf("Waiting for read lock on file %s\n", sdfsFileName)
					time.Sleep(global.RETRY_LOCK_SLEEP_TIME)
				} else {
					shouldWaitForLock = false
					resp = r
				}
			} else {
				time.Sleep(global.RETRY_CONN_SLEEP_TIME)
				fmt.Printf("Retrying to get file %s\n", sdfsFileName)
			}
		}

		if shouldWaitForLock {
			// Failed to acquire lock or reach leader, retry
			continue
		}

		replicas := resp.VMAddresses
		if len(replicas) == 0 {
			fmt.Printf("No target read replicas provided\n")
		}
		for _, r := range replicas {
			fmt.Printf("Trying to get file %s from replica: %s\n", sdfsFileName, r)
			remotePath := getScpHostNameFromHostName(r) + ":" + filepath.Join(SDFS_PATH, sdfsFileName)
			//// limited the speed to 30MB/s
			cmd := exec.Command("scp", remotePath, localFileName)
			// cmd := exec.Command("scp", remotePath, localFileName)
			err := cmd.Start()
			if err != nil {
				fmt.Printf("Failed to start command: %v\n", err)
				return
			}

			err = cmd.Wait()
			if err != nil {
				fmt.Printf("Get Command finished with error: %v\n", err)
				continue
			}
			break
		}

		sendGetACKToLeader(sdfsFileName)
		readOperationTime := time.Since(readStartTime).Milliseconds()
		fmt.Printf("Successfully get file %s in %v ms \n", sdfsFileName, readOperationTime)
		conn.Close()
		break
	}
}

func sendGetACKToLeader(sdfsFileName string) {
	var conn *grpc.ClientConn
	var c pb.SDFSClient
	var err error

	for {
		if conn == nil {
			ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer dialCancel()
			conn, err = grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("did not connect: %v\n", err)
				continue
			}
			c = pb.NewSDFSClient(conn)
		}

		// Add request max response time limit
		timeout := 3 * time.Second
		ctx, callCancel := context.WithTimeout(context.Background(), timeout)
		defer callCancel()

		ackResponse, err := c.GetACK(ctx, &pb.GetACKRequest{
			RequesterAddress: HOSTNAME,
			FileName:         sdfsFileName,
		})
		if err != nil {
			fmt.Printf("Leader failed to process get ACK: %v\n", err)
			conn.Close()
			conn = nil
			time.Sleep(global.RETRY_CONN_SLEEP_TIME)
			fmt.Printf("Retrying to send get ACK to leader %s\n", sdfsFileName)
			continue
		}
		if ackResponse == nil || !ackResponse.Success {
			fmt.Printf("Leader process get ACK unsuccessfully: %v\n", err)
			conn.Close()
			return
		}
		fmt.Printf("Leader processed get ACK successfully \n")
		conn.Close()
		break
	}
}

func HandlePutFile(localFileName string, sdfsFileName string) {
	if _, err := os.Stat(localFileName); os.IsNotExist(err) {
		fmt.Printf("Local file not exist: %s\n", localFileName)
		return
	}

	var conn *grpc.ClientConn
	var c pb.SDFSClient
	var err error
	putStartTime := time.Now()
	for {
		// Establish a new connection if it doesn't exist or previous leader failed
		if conn == nil {
			ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer dialCancel()
			conn, err = grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("did not connect: %v\n", err)
				continue
			}
			c = pb.NewSDFSClient(conn)
		}

		shouldWaitForLock := true
		resp := &pb.PutResponse{}
		for shouldWaitForLock {
			// Add request max response time limit
			timeout := 3 * time.Second
			ctx, callCancel := context.WithTimeout(context.Background(), timeout)
			defer callCancel()

			r, err := c.PutFile(ctx, &pb.PutRequest{
				RequesterAddress: HOSTNAME,
				FileName:         sdfsFileName,
			})

			if err != nil {
				fmt.Printf("Failed to call put: %v\n", err)
				// Close the connection and break to outer loop to retry
				conn.Close()
				conn = nil
				time.Sleep(global.RETRY_CONN_SLEEP_TIME)
				fmt.Printf("Retrying to get file %s\n", sdfsFileName)
				break
			}

			if r != nil {
				if !r.Success {
					fmt.Printf("Failed to put file %s to sdfs %s \n", localFileName, sdfsFileName)
					conn.Close()
					return
				}

				if r.ShouldWait {
					fmt.Printf("Waiting for write lock on file %s\n", sdfsFileName)
					time.Sleep(global.RETRY_LOCK_SLEEP_TIME)
				} else {
					shouldWaitForLock = false
					resp = r
				}
			} else {
				time.Sleep(global.RETRY_CONN_SLEEP_TIME)
				fmt.Printf("Retrying to get file %s\n", sdfsFileName)
			}
		}

		if shouldWaitForLock {
			// Failed to acquire lock or reach leader, retry
			continue
		}

		targetReplicas := resp.VMAddresses
		if len(targetReplicas) == 0 {
			fmt.Printf("No target write replicas provided\n")
			conn.Close()
			return
		}

		fmt.Printf("Starting to put file: %s to SDFS file: %s \n", localFileName, sdfsFileName)
		err = transferFilesConcurrent(localFileName, sdfsFileName, targetReplicas)
		if err != nil {
			fmt.Printf("Failed to transfer file: %v\n", err)
		}
		lineCount, err := global.CountFileLines(localFileName)
		sendPutACKToLeader(lineCount, sdfsFileName, targetReplicas, false, false)
		if err != nil {
			fmt.Printf("Failed to count lines: %v\n", err)
		} else {
			putOperationTime := time.Since(putStartTime).Milliseconds()
			fmt.Printf("Successfully put file %s to SDFS file %s in %v ms\n", localFileName, sdfsFileName, putOperationTime)
		}
		conn.Close()
		break
	}
}

// Similar to PUT, but for file append
func HandleAppendFile(sdfsFileName string, content string) {
	var conn *grpc.ClientConn
	var c pb.SDFSClient
	var err error
	version := rand.Intn(1 << 30)
	startTime := time.Now()
	for {
		// Establish a new connection if it doesn't exist or previous leader failed
		if conn == nil {
			ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer dialCancel()
			conn, err = grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("did not connect: %v\n", err)
				continue
			}
			c = pb.NewSDFSClient(conn)
		}

		shouldWaitForLock := true
		resp := &pb.PutResponse{}
		for shouldWaitForLock {
			// Add request max response time limit
			// timeout := 3 * time.Second
			// ctx, callCancel := context.WithTimeout(context.Background(), timeout)
			// defer callCancel()
			// Reusing PutFile() because lock acquiring process should be the same
			r, err := c.PutFile(context.Background(), &pb.PutRequest{
				RequesterAddress: HOSTNAME,
				FileName:         sdfsFileName,
			})

			if err != nil {
				fmt.Printf("Failed to call append: %v\n", err)
				// Close the connection and break to outer loop to retry
				conn.Close()
				conn = nil
				time.Sleep(global.RETRY_CONN_SLEEP_TIME)
				fmt.Printf("Retrying to append file %s\n", sdfsFileName)
				break
			}

			if r != nil {
				if !r.Success {
					fmt.Printf("Failed to append file sdfs %s \n", sdfsFileName)
					conn.Close()
					return
				}

				if r.ShouldWait {
					fmt.Printf("Waiting for write lock on file %s\n", sdfsFileName)
					time.Sleep(global.RETRY_LOCK_SLEEP_TIME)
				} else {
					shouldWaitForLock = false
					resp = r
				}
			} else {
				time.Sleep(global.RETRY_CONN_SLEEP_TIME)
				fmt.Printf("Retrying to append file %s\n", sdfsFileName)
			}
		}

		if shouldWaitForLock {
			// Failed to acquire lock or reach leader, retry
			continue
		}

		targetVMAddrs := resp.VMAddresses
		if len(targetVMAddrs) == 0 {
			fmt.Printf("No target VM of append provided\n")
			conn.Close()
			return
		}

		fmt.Printf("Starting to append to SDFS file: %s \n", sdfsFileName)
		// err = transferFilesConcurrent(localFileName, sdfsFileName, targetVMAddrs)
		err = sendAppendContentToVMs(content, sdfsFileName, targetVMAddrs, version)
		if err != nil {
			fmt.Printf("Failed to transfer file: %v\n", err)
		}
		lineCount, err := global.CountStringLines(content)
		sendPutACKToLeader(lineCount, sdfsFileName, targetVMAddrs, false, true)
		if err != nil {
			fmt.Printf("Failed to count lines: %v\n", err)
		} else {
			putOperationTime := time.Since(startTime).Milliseconds()
			fmt.Printf("Successfully append to SDFS file %s in %v ms\n", sdfsFileName, putOperationTime)
		}
		conn.Close()
		break
	}
}

func sendAppendContentToVMs(content string, sdfsFileName string, targetVMAddrs []string, version int) error {
	var wg sync.WaitGroup
	var transferErrors []error
	var mut sync.Mutex

	for _, r := range targetVMAddrs {
		wg.Add(1)
		go func(hostname string) {
			defer wg.Done()
			err := callAppendEndpoint(content, sdfsFileName, hostname, version)
			if err != nil {
				mut.Lock()
				transferErrors = append(transferErrors, err)
				mut.Unlock()
			}
		}(r)
	}

	wg.Wait()

	if len(transferErrors) > 0 {
		return fmt.Errorf("some transfers failed: %v", transferErrors)
	}

	fmt.Printf("Successfully append file to VMs: %+q\n", targetVMAddrs)
	return nil
}

func callAppendEndpoint(content string, sdfsFileName string, targetHostname string, version int) error {
	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(ctx, targetHostname+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
		return errors.New("[callAppendEndpoint]: Cannot connect to target: " + targetHostname)
	}
	c := pb.NewSDFSClient(conn)

	// Add request max response time limit
	timeout := 3 * time.Second
	ctx, callCancel := context.WithTimeout(context.Background(), timeout)
	defer callCancel()

	r, err := c.AppendNewContent(ctx, &pb.AppendNewContentRequest{
		FileName: sdfsFileName,
		Content:  content,
		Version:  int32(version),
	})

	if err != nil {
		return err
	}
	if !r.Success {
		return errors.New("[callAppendEndpoint]: append response returns non-successful from " + targetHostname)
	}

	return nil
}

func transferFilesConcurrent(localFileName string, sdfsFileName string, targetReplicas []string) error {
	var wg sync.WaitGroup
	var transferErrors []error
	var mut sync.Mutex

	for _, r := range targetReplicas {
		wg.Add(1)
		go func(replica string) {
			defer wg.Done()
			err := transferFileToReplica(localFileName, sdfsFileName, replica)
			if err != nil {
				mut.Lock()
				transferErrors = append(transferErrors, err)
				mut.Unlock()
			}
		}(r)
	}

	wg.Wait()

	if len(transferErrors) > 0 {
		return fmt.Errorf("some transfers failed: %v", transferErrors)
	}

	fmt.Printf("Successfully put file to replicas: %+q\n", targetReplicas)
	return nil
}

func transferFileToReplica(localFileName string, sdfsFileName string, replica string) error {
	targetHostName := getScpHostNameFromHostName(replica)
	remotePath := targetHostName + ":" + filepath.Join(SDFS_PATH, sdfsFileName)
	////limited the speed to 30MB/s
	cmd := exec.Command("scp", localFileName, remotePath)
	// cmd := exec.Command("scp", localFileName, remotePath)
	err := cmd.Start()
	if err != nil {
		fmt.Printf("Failed to start command: %v\n", err)
		return err
	}

	err = cmd.Wait()
	if err != nil {
		fmt.Printf("Transfer Command finished with error: %v\n", err)
	}

	return err
}

func sendPutACKToLeader(lineCount int, sdfsFileName string, targetReplicas []string, isReplicate bool, isAppend bool) {
	var conn *grpc.ClientConn
	var c pb.SDFSClient
	var err error

	for {
		if conn == nil {
			ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer dialCancel()
			conn, err = grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("did not connect: %v\n", err)
				continue
			}
			c = pb.NewSDFSClient(conn)
		}
		// Add request max response time limit
		timeout := 3 * time.Second
		ctx, callCancel := context.WithTimeout(context.Background(), timeout)
		defer callCancel()

		r, err := c.PutACK(ctx, &pb.PutACKRequest{
			LineCount:        int64(lineCount),
			FileName:         sdfsFileName,
			ReplicaAddresses: targetReplicas,
			IsReplicate:      isReplicate,
			IsAppend:         isAppend,
			RequesterAddress: HOSTNAME,
		})
		if err != nil {
			fmt.Printf("Leader failed to process put ACK: %v\n", err)
			conn.Close()
			conn = nil
			time.Sleep(global.RETRY_CONN_SLEEP_TIME)
			fmt.Printf("Retrying to send put ACK to leader %s\n", sdfsFileName)
			continue
		}
		if r == nil || !r.Success {
			fmt.Printf("Leader process put ACK unsuccessfully: %v\n", err)
			conn.Close()
			return
		}
		fmt.Printf("Leader processed put ACK successfully \n")
		conn.Close()
		break
	}
}

func HandleDeleteFile(sdfsFileName string) {
	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	defer conn.Close()

	c := pb.NewSDFSClient(conn)
	// Add request max response time limit
	timeout := 3 * time.Second
	ctx, callCancel := context.WithTimeout(context.Background(), timeout)
	defer callCancel()
	r, err := c.DeleteFileLeader(ctx, &pb.DeleteRequestLeader{
		FileName: sdfsFileName,
	})

	if err != nil {
		fmt.Printf("Failed to call delete: %v\n", err)
	}
	if r.Success {
		fmt.Printf("Successfully deleted file %s\n", sdfsFileName)
	} else {
		fmt.Printf("Failed to delete file %s\n", sdfsFileName)
	}
}

func HandleListFileHolders(sdfsFileName string) {
	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	defer conn.Close()

	c := pb.NewSDFSClient(conn)
	timeout := 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	r, err := c.ListFileHolder(ctx, &pb.ListFileHolderRequest{
		FileName: sdfsFileName,
	})

	if err != nil {
		// Check if the error is due to a timeout
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Printf("gRPC call timed out after %s\n", timeout)
		} else {
			fmt.Printf("Failed to call ls: %v\n", err)
		}
	}
	if r.Success {
		fmt.Printf("File %v has %v lines\n", sdfsFileName, r.LineCount)
		fmt.Printf("%+q\n", r.VMAddresses)
	} else {
		fmt.Printf("Failed to list VMs for file: %s\n", sdfsFileName)
	}
}

func HandleListLocalFiles() {
	ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(ctx, global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	defer conn.Close()

	c := pb.NewSDFSClient(conn)
	timeout := 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	r, err := c.ListLocalFiles(ctx, &pb.ListLocalFilesRequest{
		SenderAddress: HOSTNAME,
	})

	if err != nil {
		// Check if the error is due to a timeout
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Printf("gRPC call timed out after %s\n", timeout)
		} else {
			fmt.Printf("Failed to call store: %v\n", err)
		}
	}
	if r.Success {
		fmt.Printf("%+q\n", r.FileNames)
	} else {
		fmt.Printf("Failed to list local files for machine %s\n", HOSTNAME)
	}
}

func LaunchMultiReads(sdfsFileName string, localFileName string, targetVMIDs []string) {
	startTime := time.Now()
	var wg sync.WaitGroup
	var errors []error
	var mut sync.Mutex
	for _, ID := range targetVMIDs {
		wg.Add(1)
		go func(vmAddr string) {
			defer wg.Done()
			ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer dialCancel()
			conn, err := grpc.DialContext(ctx, vmAddr+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("did not connect: %v\n", err)
				mut.Lock()
				errors = append(errors, err)
				mut.Unlock()
			}
			c := pb.NewSDFSClient(conn)
			r, err := c.MultiGetFile(context.Background(), &pb.MultiGetRequest{
				SdfsFileName:  sdfsFileName,
				LocalFileName: localFileName,
			})
			if err != nil {
				fmt.Printf("multi read finished with error: %v\n", err)
				errors = append(errors, err)
				return
			}
			if !r.Success {
				fmt.Printf("multi read ended with error: %v\n", err)
				errors = append(errors, fmt.Errorf("multi get failed for %s", vmAddr))
			}
		}(getFullHostNameFromID(ID))
	}

	wg.Wait()

	if len(errors) > 0 {
		fmt.Printf("Some multi reads failed: %v\n", errors)
	}
	operationTime := time.Since(startTime).Milliseconds()
	fmt.Printf("Finished multi read file %s in %v ms \n", sdfsFileName, operationTime)
}

func LaunchMultiWriteRead(sdfsFileName string, localFileName string, writerVMIDs []string) {
	startTime := time.Now()
	var wg sync.WaitGroup
	var errors []error
	var mut sync.Mutex
	for _, ID := range writerVMIDs {
		wg.Add(1)
		go func(vmAddr string) {
			defer wg.Done()
			ctx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer dialCancel()
			conn, err := grpc.DialContext(ctx, vmAddr+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("did not connect: %v\n", err)
				mut.Lock()
				errors = append(errors, err)
				mut.Unlock()
			}
			c := pb.NewSDFSClient(conn)
			r, err := c.MultiPutFile(context.Background(), &pb.MultiPutRequest{
				SdfsFileName:  sdfsFileName,
				LocalFileName: localFileName,
			})
			if err != nil {
				fmt.Printf("Multi write finished with error: %v\n", err)
				errors = append(errors, err)
				return
			}
			if !r.Success {
				fmt.Printf("Multi write ended with error: %v\n", err)
				errors = append(errors, fmt.Errorf("multi write failed for %s", vmAddr))
			}

		}(getFullHostNameFromID(ID))
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		HandleGetFile(sdfsFileName, "1G_Local.log")
	}()

	wg.Wait()

	if len(errors) > 0 {
		fmt.Printf("Some multi writes failed: %v\n", errors)
	}
	operationTime := time.Since(startTime).Milliseconds()
	fmt.Printf("Finished multi write-read file %s in %v ms \n", sdfsFileName, operationTime)
}

func HandlePutWithSchema(localFileName string, sdfsFileName string) {
	file, err := os.Open(localFileName)
	if err != nil {
		fmt.Printf("Failed to open file %s\n", localFileName)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		fmt.Printf("Failed to read first line of file %s\n", localFileName)
		return
	}
	schema := scanner.Text()
	schemaFileName := localFileName + "_schema"
	schemaFile, err := os.CreateTemp("", schemaFileName)
	if err != nil {
		fmt.Printf("Failed to create schema file %s\n", schemaFileName)
		return
	}
	defer os.Remove(schemaFile.Name())

	_, err = schemaFile.WriteString(schema + "\n")
	if err != nil {
		fmt.Printf("Failed to write schema to file %s\n", schemaFileName)
		return
	}
	HandlePutFile(schemaFile.Name(), sdfsFileName+"_schema")

	dataFileName := localFileName + "_data"
	dataFile, err := os.CreateTemp("", dataFileName)
	if err != nil {
		fmt.Printf("Failed to create data file %s\n", dataFileName)
		return
	}
	defer os.Remove(dataFile.Name())
	for scanner.Scan() {
		_, err = dataFile.WriteString(scanner.Text() + "\n")
		if err != nil {
			fmt.Printf("Failed to write data to new data file %s\n", dataFileName)
			return
		}
	}
	HandlePutFile(dataFile.Name(), sdfsFileName)
}

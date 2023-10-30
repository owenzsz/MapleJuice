package SDFS

import (
	"context"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SDFS file operations
func handleGetFile(sdfsFileName string, localFileName string) {
	conn, err := grpc.Dial(global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	defer conn.Close()

	c := pb.NewSDFSClient(conn)

	r, err := c.GetFile(context.Background(), &pb.GetRequest{
		FileName: sdfsFileName,
	})

	if err != nil {
		fmt.Printf("Failed to call get: %v\n", err)
	}

	if !r.Success {
		fmt.Println("failed to acquire the list of vms to read the file from")
		return
	}
	replicas := r.VMAddresses
	for _, r := range replicas {
		fmt.Printf("Trying to get file %s from replica: %s\n", sdfsFileName, r)
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
	ackResponse, err := c.GetACK(context.Background(), &pb.GetACKRequest{
		FileName: sdfsFileName,
	})
	if err != nil {
		fmt.Printf("Leader failed to process get ACK: %v\n", err)
		return
	}
	if ackResponse == nil || !ackResponse.Success {
		fmt.Printf("Leader process get ACK unsuccessfully: %v\n", err)
		return
	}
	fmt.Printf("Successfully get file %s\n", sdfsFileName)
}
func handlePutFile(localFileName string, sdfsFileName string) {
	if _, err := os.Stat(localFileName); os.IsNotExist(err) {
		fmt.Printf("Local file not exist: %s\n", localFileName)
		return
	}
	conn, err := grpc.Dial(global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
		return
	}
	defer conn.Close()

	c := pb.NewSDFSClient(conn)
	r, err := c.PutFile(context.Background(), &pb.PutRequest{
		FileName: sdfsFileName,
	})

	if err != nil {
		fmt.Printf("Failed to call put: %v\n", err)
		return
	}

	if r == nil || !r.Success {
		fmt.Printf("Failed to put file %s to sdfs %s \n", localFileName, sdfsFileName)
		return
	}

	targetReplicas := r.VMAddresses
	if len(targetReplicas) == 0 {
		fmt.Printf("No target replicas provided\n")
		return
	}

	fmt.Printf("Starting to put file: %s to SDFS file: %s \n", localFileName, sdfsFileName)
	err = transferFile(localFileName, sdfsFileName, targetReplicas)
	if err != nil {
		fmt.Printf("Failed to transfer file: %v\n", err)
	} else {
		r, err := c.PutACK(context.Background(), &pb.PutACKRequest{
			FileName:         sdfsFileName,
			ReplicaAddresses: targetReplicas,
		})
		if err != nil {
			fmt.Printf("Leader failed to process put ACK: %v\n", err)
			return
		}
		if r == nil || !r.Success {
			fmt.Printf("Leader process put ACK unsuccessfully: %v\n", err)
			return
		}
	}
}

func transferFile(localFileName string, sdfsFileName string, targetReplicas []string) error {
	var err error
	for _, r := range targetReplicas {
		targetHostName := getScpHostNameFromHostName(r)
		remotePath := targetHostName + ":" + filepath.Join(SDFS_PATH, sdfsFileName)
		cmd := exec.Command("scp", localFileName, remotePath)
		err = cmd.Start()
		if err != nil {
			fmt.Printf("Failed to start command: %v\n", err)
		}

		err = cmd.Wait()
		if err != nil {
			fmt.Printf("Command finished with error: %v\n", err)
		}
	}
	fmt.Printf("Put file to replicas: %+q\n", targetReplicas)
	return err
}

func handleDeleteFile(sdfsFileName string) {
	conn, err := grpc.Dial(global.GetLeaderAddress()+":"+global.SDFS_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
	}
	defer conn.Close()

	c := pb.NewSDFSClient(conn)
	r, err := c.DeleteFileLeader(context.Background(), &pb.DeleteRequestLeader{
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

func handleListFileHolders(sdfsFileName string) {
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
		fmt.Printf("%+q\n", r.VMAddresses)
	} else {
		fmt.Printf("Failed to list VMs for file: %s\n", sdfsFileName)
	}
}

func handleListLocalFiles() {
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

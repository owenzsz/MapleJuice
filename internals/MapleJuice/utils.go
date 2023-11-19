package maplejuice

import (
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"fmt"
	"math"
)

func generateMapleWorkersList(numMaples int) []string {
	//TODO: This is a temporary implementation, we need to change this to a more sophisticated one
	workersList := make([]string, numMaples)
	for i := 0; i < numMaples; i++ {
		workersList[i] = global.SERVER_ADDRS[i]
	}
	return workersList
}

func calculateMapleWorkload(files []string, numMaples int) (int, error) {
	totalLines := 0
	for _, file := range files {
		lineCount, ok := global.MemTable.FileLineCountMap[file]
		if !ok {
			err := fmt.Errorf("file %s not found in MemTable", file)
			return 0, err
		}
		totalLines += lineCount
	}
	quotient := float64(totalLines) / float64(numMaples)
	individualWorkload := int(math.Ceil(quotient))
	return individualWorkload, nil
}

// For each worker, assign a list of files and line ranges to process in the maple task
func assignMapleWorkToWorkers(dir string, numMaples int) []*pb.MapleWorkerListeResponse_WorkerTaskAssignment {
	workersList := generateMapleWorkersList(numMaples)
	files := global.ListAllFilesInDirectory(dir)
	linesPerWorker, err := calculateMapleWorkload(files, numMaples)
	if err != nil {
		fmt.Printf("Failed to calculate maple workload: %v\n", err)
		return nil
	}
	assignments := make([]*pb.MapleWorkerListeResponse_WorkerTaskAssignment, numMaples)
	for i, workerAddress := range workersList {
		assignments[i] = &pb.MapleWorkerListeResponse_WorkerTaskAssignment{
			WorkerAddress: workerAddress,
		}
	}
	currentWorker := 0
	currentLines := 0

	for _, file := range files {
		lineCount, ok := global.MemTable.FileLineCountMap[file]
		if !ok {
			fmt.Printf("File %s not found in MemTable\n", file)
			continue
		}
		startLine := 0
		for lineCount > 0 {
			remainingLinesForWorker := linesPerWorker - currentLines
			linesToTake := global.Min(lineCount, remainingLinesForWorker)

			// Add file and line range to current worker's assignment
			endLine := startLine + linesToTake - 1
			fileLines := &pb.FileLines{
				Filename: file,
				Range: &pb.LineRange{
					Start: int32(startLine),
					End:   int32(endLine),
				},
			}
			assignments[currentWorker].Files = append(assignments[currentWorker].Files, fileLines)

			lineCount -= linesToTake
			startLine += linesToTake
			currentLines += linesToTake

			if currentLines >= linesPerWorker {
				currentWorker++
				if currentWorker >= numMaples {
					currentWorker = 0 // Reset to first worker if we have more lines than workers can handle
				}
				currentLines = 0
			}
		}
	}

	return assignments

}

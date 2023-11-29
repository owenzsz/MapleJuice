package maplejuice

import (
	"bufio"
	"crypto/md5"
	fd "cs425-mp/internals/failureDetector"
	"cs425-mp/internals/global"
	pb "cs425-mp/protobuf"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

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
func assignMapleWorkToWorkers(dir string, numMaples int) map[string][]*pb.FileLines {
	randomNodeList := fd.RandomlySelectNodes(numMaples, global.GetLeaderAddress())
	workersList := make([]string, 0)
	for _, node := range randomNodeList {
		workersList = append(workersList, node.NodeAddr)
	}
	files := global.ListAllFilesInDirectory(dir)
	linesPerWorker, err := calculateMapleWorkload(files, numMaples)
	if err != nil {
		fmt.Printf("Failed to calculate maple workload: %v\n", err)
		return nil
	}
	assignments := make(map[string][]*pb.FileLines, numMaples)
	for _, workerAddress := range workersList {
		assignments[workerAddress] = make([]*pb.FileLines, 0)
	}
	currentWorkerIndex := 0
	currentLines := 0
	currentWorker := workersList[currentWorkerIndex]

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
			assignments[currentWorker] = append(assignments[currentWorker], fileLines)

			lineCount -= linesToTake
			startLine += linesToTake
			currentLines += linesToTake

			if currentLines >= linesPerWorker {
				currentWorkerIndex++
				if currentWorkerIndex >= numMaples {
					currentWorkerIndex = 0 // Reset to first worker if we have more lines than workers can handle
				}
				currentWorker = workersList[currentWorkerIndex]
				currentLines = 0
			}
		}
	}
	fmt.Printf("Maple assignments: %v\n", assignments)
	return assignments

}

func getAndSortAllFilesWithPrefix(prefix string) []string {
	res := make([]string, 0)
	global.MemtableLock.Lock()
	for filename := range global.MemTable.FileToVMMap {
		if strings.HasPrefix(filename, prefix) {
			res = append(res, filename)
		}
	}
	global.MemtableLock.Unlock()
	sort.Strings(res)
	return res
}

func createKeyAssignmentForJuicers(numJuicer int, filePrefix string, useRangePartition bool) map[string]map[string]global.Empty {
	keyAssignment := make(map[string]map[string]global.Empty)
	// Get all intermediate files with filePrefix, the returning list is sorted
	intermediateFiles := getAndSortAllFilesWithPrefix(filePrefix)

	// Randomly get numJuicer number of VMs
	selectedNode := fd.RandomlySelectNodes(numJuicer, global.GetLeaderAddress())

	// Using range partitioning to assign intermediate files
	if useRangePartition {
		numKeys := len(intermediateFiles)
		load := numKeys / numJuicer
		remainder := numKeys % numJuicer
		if remainder != 0 {
			load++
		}

		fileIndex := 0
		for i := 0; i < numJuicer; i++ {
			workerAddr := selectedNode[i].NodeAddr
			numKeyGiven := 0
			for fileIndex < len(intermediateFiles) && numKeyGiven < load {
				filename := intermediateFiles[fileIndex]
				if keyAssignment[workerAddr] == nil {
					keyAssignment[workerAddr] = map[string]global.Empty{}
				}
				keyAssignment[workerAddr][filename] = global.Empty{}
				fileIndex++
				numKeyGiven++
			}
		}
	} else {
		// Using hash partitioning to assign intermediate files
		for _, filename := range intermediateFiles {
			hasher := md5.New()
			hasher.Write([]byte(filename))
			checksum := hasher.Sum(nil)
			// checksum = checksum[len(checksum)-8:]
			hexString := hex.EncodeToString(checksum)
			hexString = "0" + hexString[len(hexString)-7:]
			value, err := strconv.ParseInt(hexString, 16, 32)
			if err != nil {
				fmt.Printf("error in hash partitioning juice task: %v\n", err)
			}
			assignee := selectedNode[value%int64(len(selectedNode))]
			if keyAssignment[assignee.NodeAddr] == nil {
				keyAssignment[assignee.NodeAddr] = map[string]global.Empty{}
			}
			keyAssignment[assignee.NodeAddr][filename] = global.Empty{}
		}
	}

	return keyAssignment
}

func generateFilterMapleExeFileWithRegex(regex string, schema string, field string) (string, error) {
	pythonScript := fmt.Sprintf(`
import sys
import re

schema = "%s".split(',')
field = "%s"
regex = re.compile(r"%s")

def process_line(line):
	line = line.strip().split("##")[1]
	data = dict(zip(schema, line.strip().split(',')))
	if field in data and regex.search(data[field]):
		print(f"key:{line}")

if __name__ == "__main__":
	for line in sys.stdin:
		process_line(line)		
`, schema, field, regex)

	fileName := "SQL_filter_map.py"
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error creating Python file:", err)
		return "", err
	}
	defer file.Close()

	_, err = file.WriteString(pythonScript)
	if err != nil {
		fmt.Println("Error writing to Python file:", err)
		return "", err
	}

	return fileName, nil
}

func generateJoinMapleExeFile(table1 string, column1 string, schema1 string, table2 string, column2 string, schema2 string) (string, error) {
	pythonScript := fmt.Sprintf(`
import sys

dataset1 = "%s"
dataset2 = "%s"
schema1 = "%s".split(',')
schema2 = "%s".split(',') 
col_name1 = "%s"
col_name2 = "%s"


def process_line(line):
	line_splitted = line.strip().split("##")
	dataset = line_splitted[0]
	row = line_splitted[1]

	if dataset == dataset1:
		data = dict(zip(schema1, row.strip().split(',')))
		if col_name1 in data:
			col_value = data[col_name1]
			print(f"{col_value}:{dataset}->{row}")
	elif dataset == dataset2:
		data = dict(zip(schema2, row.strip().split(',')))
		if col_name2 in data:
			col_value = data[col_name2]
			print(f"{col_value}:{dataset}->{row}")

if __name__ == "__main__":
	for line in sys.stdin:
		process_line(line)
`, table1, table2, schema1, schema2, column1, column2)
	
	fileName := "SQL_join_map.py"
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error creating Python file:", err)
		return "", err
	}
	defer file.Close()

	_, err = file.WriteString(pythonScript)
	if err != nil {
		fmt.Println("Error writing to Python file:", err)
		return "", err
	}

	return fileName, nil
}

func generateJuiceFilterExeFile() (string, error) {
	pythonScript := `
import sys

def process_line(line):
    key, value_set = line.strip().split(':', 1)
    agg_result = [x for x in value_set.split("::")]
    for result in agg_result:
        print(f"{result}")


if __name__ == "__main__":
    for line in sys.stdin:
        process_line(line)
`

	// Write the Python script to a file
	fileName := "SQL_filter_reduce.py"
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error creating Python file:", err)
		return "", err
	}
	defer file.Close()

	_, err = file.WriteString(pythonScript)
	if err != nil {
		fmt.Println("Error writing to Python file:", err)
		return "", err
	}

	return fileName, nil
}

func generateJoinJuiceExeFile() (string, error) {
	pythonScript := `
import sys

def process_line(line):
    # line format: {col_value}:{dataset}->{row}::{dataset}->{row}::...
    key, value_set = line.strip().split(':', 1)
    value_splitted = value_set.split("::")
    datasets_names = set([x.split("->")[0] for x in value_splitted])
    datasets_names_list = sorted(datasets_names)
    
    if len(datasets_names) == 2:
        value_from_table_a = [x.split("->")[1] for x in value_splitted if (x.split("->")[0] == datasets_names_list[0])]
        value_from_table_b = [x.split("->")[1] for x in value_splitted if (x.split("->")[0] == datasets_names_list[1])]
        # All combination
        for i in range(len(value_from_table_a)):
            for j in range(len(value_from_table_b)):
                rowL, rowR = value_from_table_a[i], value_from_table_b[j]
                print(f"{datasets_names_list[0]}->{rowL},{datasets_names_list[1]}->{rowR}")


if __name__ == "__main__":
    for line in sys.stdin:
        process_line(line)
`
	// Write the Python script to a file
	fileName := "SQL_join_reduce.py"
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error creating Python file:", err)
		return "", err
	}
	defer file.Close()

	_, err = file.WriteString(pythonScript)
	if err != nil {
		fmt.Println("Error writing to Python file:", err)
		return "", err
	}

	return fileName, nil	
}

func extractSchemaFromSchemaFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", filename, err)
		return "", err
	}
	defer file.Close()

	// Read the first line of the file
	// var schema string
	// _, err = fmt.Fscanf(file, "%s\n", &schema)
	// if err != nil {
	// 	fmt.Printf("Error reading schema from file %s: %v\n", filename, err)
	// 	return "", err
	// }

	scanner := bufio.NewScanner(file)
	scanner.Scan()
	schema := scanner.Text()
	if err := scanner.Err(); err != nil {
        return "", err
    }
	schema = strings.TrimPrefix(schema, "\uFEFF")
	return schema, nil
}

// Adapted from https://golangbyexample.com/longest-common-prefix-golang/
func longestCommonPrefix(strs []string) string {
	lenStrs := len(strs)

	if lenStrs == 0 {
		return ""
	}

	firstString := strs[0]

	lenFirstString := len(firstString)

	commonPrefix := ""
	for i := 0; i < lenFirstString; i++ {
		firstStringChar := string(firstString[i])
		match := true
		for j := 1; j < lenStrs; j++ {
			if (len(strs[j]) - 1) < i {
				match = false
				break
			}

			if string(strs[j][i]) != firstStringChar {
				match = false
				break
			}

		}

		if match {
			commonPrefix += firstStringChar
		} else {
			break
		}
	}

	return commonPrefix
}
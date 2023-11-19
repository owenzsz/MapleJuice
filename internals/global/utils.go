package global

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// adapted from: https://stackoverflow.com/questions/10485743/contains-method-for-a-slice
func Contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}

func RemoveElementWithRange[T comparable](s []T, e T, start int, end int) ([]T, error) {
	if len(s) == 0 {
		return s, nil
	}
	end = Min(end, len(s)-1)
	if start < 0 {
		return nil, fmt.Errorf("invalid range")
	}
	var idx int
	var found bool
	for i := start; i <= end; i++ {
		v := s[i]
		if v == e {
			idx = i
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("element not found in the specified range")
	}
	return append(s[:idx], s[idx+1:]...), nil
}

func CountStringLines(content string) (int, error) {
	scanner := bufio.NewScanner(strings.NewReader(content))
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	return lineCount, scanner.Err()
}

func CountFileLines(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	return lineCount, scanner.Err()
}

func ListAllFilesInDirectory(srcDirectory string) []string {
	var inputFiles []string
	MemtableLock.Lock()
	for key := range MemTable.FileToVMMap {
		if strings.HasPrefix(key, srcDirectory) {
			inputFiles = append(inputFiles, key)
		}
	}
	MemtableLock.Unlock()
	return inputFiles
}

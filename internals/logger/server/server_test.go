package server

import (
	"net"
	"strings"
	"testing"
)

// common components for all test to test serveConn
func runServeConnTest(t *testing.T, request string, conditionFunc func(string) bool, expectedResponse string) {
	out := make([]byte, 1024)
	//use net.pipe() to mock conn
	read, write := net.Pipe()
	go serveConn(read)
	defer write.Close()

	n, err := write.Write([]byte(request))
	if err != nil || n == 0 {
		t.Fatalf("Failed to write to pipe: %v", err)
	}

	n, err = write.Read(out)
	if err != nil || n == 0 {
		t.Fatalf("Failed to read from pipe: %v", err)
	}

	trimmedOut := string(out[:n])
	if !conditionFunc(trimmedOut) {
		t.Errorf("Unexpected response: got %q, expected %q", trimmedOut, expectedResponse)
	}
}

func TestServeConnInvalidRequest(t *testing.T) {
	expectedResponse := "Error: Invalid request. Please try again.\n"
	conditionFunc := func(out string) bool {
		return out == expectedResponse
	}
	runServeConnTest(t, "NotARequest \n", conditionFunc, expectedResponse)
}

func TestServeConnExistRequest(t *testing.T) {
	conditionFunc := func(out string) bool {
		return strings.Contains(out, "CS_425")
	}
	runServeConnTest(t, "grep CS_425 ../../fake_log.log \n", conditionFunc, "Expected the response to contain CS_425")
}

func TestServeConnNonExistRequest(t *testing.T) {
	conditionFunc := func(out string) bool {
		return !strings.Contains(out, "Test_Log")
	}
	runServeConnTest(t, "grep TEST_LOG ../../fake_log.log \n", conditionFunc, "Expected the response to not contain Test_Log")
}

// unit test for the process request function
func TestProcessRequest(t *testing.T) {
	existWordRequest := "grep CS_425 ../../fake_log.log"
	existWordResponse := processRequest(existWordRequest)
	if !strings.Contains(string(existWordResponse), "CS_425") {
		t.Errorf("Expected the response to contain CS_425, got %s", existWordResponse)
	}

	nonExistWord := "grep CS_525 ../../fake_log.log"
	nonExistWordResponse := processRequest(nonExistWord)
	if strings.Contains(string(nonExistWordResponse), "CS_525") {
		t.Errorf("Unexpected Match found, got %s", nonExistWordResponse)
	}
}

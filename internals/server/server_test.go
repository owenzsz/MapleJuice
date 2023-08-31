package server

import (
	"strings"
	"testing"
)

func TestProcessRequest(t *testing.T) {
	existWordRequest := "grep CS_425"
	existWordResponse := processRequest(existWordRequest)
	if !strings.Contains(string(existWordResponse), "CS_425") {
		t.Errorf("Expected the response to contain CS_425, got %s", existWordResponse)
	}

	nonExistWord := "grep CS_525"
	nonExistWordResponse := processRequest(nonExistWord)
	if strings.Contains(string(nonExistWordResponse), "CS_525") {
		t.Errorf("Unexpected Match found, got %s", nonExistWordResponse)
	}
}

package failureDetector

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Toggle to Gossip+Suspicion mode
func enableSuspicionHandler(w http.ResponseWriter, r *http.Request) {
	NodeListLock.Lock()
	USE_SUSPICION = true
	T_FAIL = 2 * time.Second
	GOSSIP_RATE = 400 * time.Millisecond
	fmt.Fprintf(w, "Toggled USE_SUSPICION to %v\n", USE_SUSPICION)
	NodeListLock.Unlock()
}

// Toggle to Gossip+Suspicion mode
func disableSuspicionHandler(w http.ResponseWriter, r *http.Request) {
	NodeListLock.Lock()
	USE_SUSPICION = false
	T_FAIL = 4 * time.Second
	fmt.Fprintf(w, "Toggled USE_SUSPICION to %v\n", USE_SUSPICION)
	NodeListLock.Unlock()
}

// Message drop rate handler
func messageDropRateHandler(w http.ResponseWriter, r *http.Request) {
	newDropRate := r.URL.Query().Get("value")
	if len(newDropRate) == 0 {
		http.Error(w, "Error: Did not include required input parameter", http.StatusBadRequest)
		return
	}
	parsedValue, err := strconv.ParseFloat(newDropRate, 64)
	if err != nil {
		http.Error(w, "Error: Invalid ihnput parameter value type", http.StatusBadRequest)
		return
	}
	NodeListLock.Lock()
	MESSAGE_DROP_RATE = parsedValue
	NodeListLock.Unlock()
	fmt.Fprintf(w, "Change message drop rate to %v\n", parsedValue)
}

func HandleExternalSignals() {
	http.HandleFunc("/enableSuspicion", enableSuspicionHandler)
	http.HandleFunc("/disableSuspicion", disableSuspicionHandler)
	http.HandleFunc("/updateMessageDropRate", messageDropRateHandler)
	http.ListenAndServe("0.0.0.0:8080", nil)
}

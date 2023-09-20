package failureDetector

import (
	"fmt"
	"net/http"
	"strconv"
)

//Suspicion toggle
func toggleSuspicionHandler(w http.ResponseWriter, r *http.Request) {
	NodeListLock.Lock()
	USE_SUSPICION = !USE_SUSPICION
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
	http.HandleFunc("/toggleSuspicion", toggleSuspicionHandler)
	http.HandleFunc("/updateMessageDropRate", messageDropRateHandler)
	http.ListenAndServe("0.0.0.0:8080", nil)
}

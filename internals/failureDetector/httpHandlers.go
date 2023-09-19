package failureDetector

import (
	"fmt"
	"net/http"
	"strconv"
)

//Suspicion toggle

func toggleSuspicionHandler(w http.ResponseWriter, r *http.Request) {
	USE_SUSPICION = !USE_SUSPICION
	fmt.Fprintf(w, "Toggled USE_SUSPICION to %v\n", USE_SUSPICION)
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
	MESSAGE_DROP_RATE = parsedValue
}

func HandleExternalSignals() {
	http.HandleFunc("/toggleSuspicion", toggleSuspicionHandler)
	http.HandleFunc("/updateMessageDropRate", messageDropRateHandler)
	http.ListenAndServe("0.0.0.0:8080", nil)
}

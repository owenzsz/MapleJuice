# CS425-MP1

## To run
1. Power on all VMs, change directory to the root folder of this project (named cs425-mp1)
2. On all VMs, run `go run cmd/cs425-mp1/main.go`. The process acts as both a server (executing grep commands) and a client (sending remote grep commands)
3. **To run the demo**: enter a grep command in the any one of the VM terminals, you should see the grep command being executed and the result is printed out to stdout together with a set of summary statistics of matched lines and latency report. 
4. **To run the unit tests**: go to the root directory of this project and run `go test ./internals/server` to run the unit tests for some of our server functions. We used mocked network connections instead of real grep requsts in unit tests
5. **To run the end-to-end tests**: go to the root directory of this project and run `python3 log_test.py`. This will run a more comprehensive test where we generate logs, distribute logs to each VM, and run our distributed grep program to see if the result matches our expectations. 
6. The report is located at the root directory of this project. The name of the file is: CS-425 MP1 Report.pdf

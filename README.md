# CS425-MP1

## TODOs

1. Sending request to other VMs (not set up yet)
2. Testing

## To run

1. Be sure about the server list in `internals/client/client.go` and the log file name  in `internals/server/server.go`
2. Using one terminal, at the root level of the project, run `go run cmd/cs425-mp1/main.go`. The process acts as both a server (executing grep commands) and a client (sending remote grep commands)
3. To test its client functionality: enter queries in the same terminal, you should see the grep command being executed (currently only on local machine), and the result is printed out to stdout.
4. To test its server functionality: on another terminal, run `netcat localhost 55555` to connect to the server. You can then enter grep commands.

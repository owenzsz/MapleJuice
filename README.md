# CS425-MP2

## Environment
* golang 1.21


## To Run
1. Log into (fa23-cs425-1801.cs.illinois.edu), change directory `cd cs425-mp2`. Run `go run cmd/cs425-mp2/main.go` to start the Introducer node
2. On other VMs, in the same directory, run `go run cmd/cs425-mp2/main.go` to start member nodes
3. We have provided a bunch of CLI commands for quick control of the group, and they are listed below:
*  **leave**. This command will ask the current VM to gracefully leave the system and enter the "LEFT" mode. That is, it neither respond to any external gossips nor send any gossip messages.
* **rejoin** This command will bring a "LEFT" node back to the group with a new node ID by sending a introduction message to the Introducer node.
* **list_mem** This command will list all the entries in the current VM's membership list. 
* **list_self** This command will list the node ID of the current VM. 
* **enable_suspicion** This command will enable the *Gossip+S* mode on every node
* **disable_suspicion** This command will enable the *Gossip* mode on every node (Default mode on start)
* **drop_rate {rate}** The command will read the {rate} variable in the user input and use it to adjust the message drop rate of the system. You can only input a floting point number between 0.0 and 1.0. 

4. The logs are written to `log.log`
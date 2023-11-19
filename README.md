# CS425-MP3

## Environment
* golang 1.21


## How to Start 
1. In directory `~/cs425-mp4` and **main** branch, run 
```
rm *.log
rm fetched*
for i in {1..20}; do
  dd if=/dev/urandom bs=1M count=20 | base64 > ${i}
done
dd if=/dev/zero of=1024  bs=1M  count=1024
git pull && \
go run cmd/cs425-mp3/main.go

```
The above commands will clean the log files, dummy files, re-generate 20 dummy files of 20MB in size, regenerate 1 dummy file of 1GB in size, and then pull the latest code and run the code.

## Supported SDFS Commands
* get [remote filename] [local filename]
* put [local filename] [remote filename]
* delete [remote filename]
* ls [remote filename]
* store
* **multiread** [remote filename] [local filename] [machine id1] [machine id2] [machine id3] ...
* list_mem
* list_self
* leave
* enable_suspicion
* disable_suspicion
* join (handled implicitly when the node starts)
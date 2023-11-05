# CS425-MP3

## Environment
* golang 1.21


## TODO
1. Implement Leader Election
2. Read/write lock (maybe a message queue in the leader server?)
3. Embed failure detector in file system. using go channel to comunicate
4. Ping/Ack for put operation
    
## Command to start
In directory: ~/cs425-mp3

run 

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


## To Run

mp3: clean generate_dummy
	git pull
	go run cmd/cs425-mp3/main.go

generate_dummy:
	for i in {1..20}; do \
		dd if=/dev/urandom bs=1M count=20 | base64 > $${i} ; \
	done
	dd if=/dev/zero of=1024 bs=1M count=1024

clean:
	rm -f ./*.log
	rm -f ./fetched*


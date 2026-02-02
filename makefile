.PHONY: build
build: clean
	go build -o kafka-replay cmd/cli/main.go

.PHONY: clean
clean:
	rm -f kafka-replay

.PHONY: record
record:
	./kafka-replay record \
		--broker=localhost:19092 \
		--topic=test-topic \
		--group-id=test-consumer-group \
		--output=messages.log \
		--limit=1000

.PHONY: replay
replay:
	./kafka-replay replay \
		--broker=localhost:19092 \
		--topic=new-topic \
		--input=messages.log \
		--create-topic

.PHONY: cat
cat:
	./kafka-replay cat \
		--input=messages.log
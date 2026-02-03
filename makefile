.PHONY: build
build: clean
	go build -o kafka-replay ./cmd/kafka-replay

.PHONY: clean
clean:
	rm -f kafka-replay

.PHONY: record
record:
	./kafka-replay record \
		--broker=redpanda:9092 \
		--topic=test-topic \
		--group-id=test-consumer-group \
		--output=messages.log \
		--limit=1000

.PHONY: replay
replay:
	./kafka-replay replay \
		--broker=redpanda:9092 \
		--topic=new-topic \
		--input=messages.log \
		--create-topic

.PHONY: cat
cat:
	./kafka-replay cat \
		--input=messages.log
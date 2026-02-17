.PHONY: build
build: clean
	go build -o kafka-replay ./cmd/kafka-replay

.PHONY: test
test:
	go test ./pkg/transcoder/...

.PHONY: integration
integration:
	go test -v -run 'TestCLI' ./cmd/kafka-replay/...

.PHONY: clean
clean:
	rm -f kafka-replay

.PHONY: record
record:
	./kafka-replay record \
		--brokers=redpanda:9092 \
		--topic=test-topic \
		--group=test-consumer-group \
		--output=messages.log \
		--limit=1000

.PHONY: replay
replay:
	./kafka-replay replay \
		--brokers=redpanda:9092 \
		--topic=new-topic \
		--input=messages.log \
		--create-topic

.PHONY: cat
cat:
	./kafka-replay cat \
		--input=messages.log
FROM golang:latest

WORKDIR /app

COPY . .

RUN go build -o kafka-replay cmd/cli/main.go

CMD ["sleep", "infinity"]
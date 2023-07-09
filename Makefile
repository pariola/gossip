clean:
	rm -rf ./bin

build-1: clean
	go build -o ./bin/main echo/main.go

test-1: build-1
	maelstrom test -w echo --bin ./bin/main --node-count 1 --time-limit 10

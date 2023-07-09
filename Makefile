clean:
	rm -rf ./bin

build-1: clean
	go build -o ./bin/main echo/main.go

test-1: build-1
	maelstrom test -w echo --bin ./bin/main --node-count 1 --time-limit 10

build-2: clean
	go build -o ./bin/main unique/main.go

test-2: build-2
	maelstrom test -w unique-ids --bin ./bin/main --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

build-3a: clean
	go build -o ./bin/main broadcast-a/main.go

test-3a: build-3a
	maelstrom test -w broadcast --bin ./bin/main --node-count 1 --time-limit 20 --rate 10

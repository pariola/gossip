package main

import (
	"log"

	"gossip/internal/broadcast"
	"gossip/internal/counter"
	"gossip/internal/echo"
	"gossip/internal/kafka"
	"gossip/internal/unique"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type handlerFn func(n *maelstrom.Node)

var (
	run     = ""
	runners = map[string]handlerFn{
		"echo":         echo.Handle,
		"unique":       unique.Handle,
		"kafka-sn":     kafka.SingleNode,
		"g-counter":    counter.Handle,
		"g-counter-2":  counter.HandleWithCommunication,
		"broadcast-a":  broadcast.HandleA,
		"broadcast-b":  broadcast.HandleB,
		"broadcast-bs": broadcast.HandleBSimple,
	}
)

func main() {
	log.Printf("running '%s'", run)

	fn, ok := runners[run]
	if !ok {
		log.Fatalf("no handler registed for '%s'", run)
	}

	defer func() {
		if err := recover(); err != nil {
			log.Println("[ERROR]", err)
		}
	}()

	node := maelstrom.NewNode()
	fn(node)
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

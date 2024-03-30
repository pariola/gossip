package main

import (
	"gossip/internal/broadcast"
	"gossip/internal/echo"
	"gossip/internal/unique"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type handlerFn func(n *maelstrom.Node)

var (
	run     = ""
	runners = map[string]handlerFn{
		"echo":         echo.Handle,
		"unique":       unique.Handle,
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

	node := maelstrom.NewNode()
	fn(node)
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

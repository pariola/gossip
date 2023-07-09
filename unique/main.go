package main

import (
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	var cur int32

	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		cur += 1

		response := map[string]any{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%s-%d-%d", n.ID(), time.Now().UnixNano(), cur),
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

package unique

import (
	"fmt"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var cur int32

func Handle(n *maelstrom.Node) {
	n.Handle("generate", func(msg maelstrom.Message) error {
		cur += 1

		response := map[string]any{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%s-%d-%d", n.ID(), time.Now().UnixNano(), cur),
		}

		return n.Reply(msg, response)
	})
}

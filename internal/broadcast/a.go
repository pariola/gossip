package broadcast

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastMsg struct {
	Message int `json:"message"`
}

type topologyMsg struct {
	Topology map[string][]string `json:"topology"`
}

func HandleA(n *maelstrom.Node) {
	messages := make([]int, 0)
	// neighbours := make([]string, 0)

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// update neighbours
		// neighbours = body.Topology[n.ID()]

		response := map[string]any{"type": "topology_ok"}
		return n.Reply(msg, response)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages = append(messages, body.Message)

		response := map[string]string{"type": "broadcast_ok"}
		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		response := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}
		return n.Reply(msg, response)
	})
}

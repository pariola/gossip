package broadcast

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func HandleBSimple(n *maelstrom.Node) {
	var rw sync.RWMutex
	messagesMap := make(map[int]struct{})

	neighbours := make([]string, 0)

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// update neighbours
		neighbours = body.Topology[n.ID()]

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := map[string]string{"type": "broadcast_ok"}

		rw.Lock()
		defer rw.Unlock()

		if _, ok := messagesMap[body.Message]; ok {
			return n.Reply(msg, response)
		}

		messagesMap[body.Message] = struct{}{}

		for _, neighbour := range neighbours {
			if neighbour == msg.Src {
				continue // skip origin
			}

			n.Send(neighbour, msg.Body)
		}

		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		rw.RLock()
		defer rw.RUnlock()

		messages := make([]int, 0, len(messagesMap))
		for message := range messagesMap {
			messages = append(messages, message)
		}

		response := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}

		return n.Reply(msg, response)
	})
}

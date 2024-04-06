package kafka

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	m      sync.Mutex
	stores map[string]*log
}

type sendMsg struct {
	Key     string `json:"key"`
	Message int64  `json:"msg"`
}

type pollMsg struct {
	Offsets map[string]int64 `json:"offsets"`
}

type commitOffsetsMsg struct {
	Offsets map[string]int64 `json:"offsets"`
}

type listCommitOffsetsMsg struct {
	Keys []string `json:"keys"`
}

func SingleNode(n *maelstrom.Node) {
	s := server{
		stores: make(map[string]*log), // initialize stores
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body sendMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		l := s.getLog(body.Key)

		offset := l.writeMessage(body.Message)

		response := map[string]any{
			"type":   "send_ok",
			"offset": offset,
		}

		return n.Reply(msg, response)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body pollMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := make(map[string][][2]int64)

		for key, offset := range body.Offsets {
			l := s.getLog(key)

			msgs := l.readMessages(offset, 10)
			if len(msgs) < 1 {
				continue
			}

			messages[key] = msgs
		}

		response := map[string]any{
			"type": "poll_ok",
			"msgs": messages,
		}

		return n.Reply(msg, response)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body commitOffsetsMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for key, offset := range body.Offsets {
			l := s.getLog(key)
			l.setCommitOffset(offset)
		}

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body listCommitOffsetsMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int64, len(body.Keys))

		for _, key := range body.Keys {
			l := s.getLog(key)
			offsets[key] = l.getCommitOffset()
		}

		response := map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		}

		return n.Reply(msg, response)
	})
}

func (s *server) getLog(name string) *log {
	if w, ok := s.stores[name]; ok {
		return w
	}

	s.m.Lock()
	defer s.m.Unlock()

	s.stores[name] = newLog()
	return s.stores[name]
}

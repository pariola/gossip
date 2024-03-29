package main

import (
	"encoding/json"
	"log"
	// "math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type (
	counter struct {
		Time      int
		LastIndex int
	}

	state struct {
		sync.RWMutex

		neighbours []string

		count    *counter
		counters map[string]*counter

		messages     []int
		seenMessages map[int]struct{}
	}

	broadcastMsg struct {
		Message int `json:"message"`
	}

	topologyMsg struct {
		Topology map[string][]string `json:"topology"`
	}

	syncRequestMsg struct {
		Type      string `json:"type"`
		Time      int    `json:"time"`
		LastIndex int    `json:"lastIndex"`
	}

	syncResponseMsg struct {
		Ok       bool               `json:"ok"`
		Type     string             `json:"type"`
		Messages []int              `json:"messages"`
		Counters map[string]counter `json:"counters"`
	}
)

func (s *state) add(m int, src string) {
	if _, ok := s.seenMessages[m]; ok {
		return
	}

	s.count.LastIndex++
	s.seenMessages[m] = struct{}{}
	s.messages = append(s.messages, m)
}

// touch updates the local counter
func (s *state) touch() {
	s.count.Time++ // increase local time
}

func main() {
	n := maelstrom.NewNode()

	s := &state{
		counters: make(map[string]*counter),

		neighbours: make([]string, 0),

		messages:     make([]int, 0),
		seenMessages: make(map[int]struct{}),
	}

	go syncer(n, s)

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.Lock()
		defer s.Unlock()

		// initialize state
		node := n.ID()

		for k := range body.Topology {
			s.counters[k] = &counter{}
		}

		s.count = s.counters[node]
		s.neighbours = body.Topology[node] // update neighbours

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.Lock()
		s.add(body.Message, msg.Src)
		s.touch()
		s.Unlock()

		response := map[string]string{"type": "broadcast_ok"}
		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		s.RLock()
		defer s.RUnlock()

		response := map[string]any{
			"type":     "read_ok",
			"messages": s.messages,
		}

		return n.Reply(msg, response)
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		var body syncRequestMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.RLock()
		defer s.RUnlock()

		// if local counter < sources view of this node
		if s.count.Time < body.Time {
			return n.Reply(msg, syncResponseMsg{Ok: false}) // should not sync
		}

		messages := s.messages[body.LastIndex:]
		if len(messages) < 1 {
			return n.Reply(msg, syncResponseMsg{Ok: false}) // should not sync
		}

		// todo: chunkify messages

		response := syncResponseMsg{
			Ok:       true,
			Counters: make(map[string]counter),
			Messages: make([]int, len(messages)),
		}

		// copy messages
		copy(response.Messages, messages)

		// copy counters
		for k, v := range s.counters {
			response.Counters[k] = *v
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func syncer(n *maelstrom.Node, s *state) {
	t := time.NewTicker(200 * time.Millisecond)
	for range t.C {
		if len(s.neighbours) < 1 || len(s.messages) < 1 { // no neighbour or no messages
			continue
		}

		for _, neighbour := range s.neighbours {
			go askSync(n, s, neighbour)
		}
	}
}

func askSync(n *maelstrom.Node, s *state, neighbour string) error {
	s.RLock()
	msg := syncRequestMsg{
		Type:      "sync",
		Time:      s.counters[neighbour].Time,
		LastIndex: s.counters[neighbour].LastIndex,
	}
	s.RUnlock()

	return n.RPC(neighbour, msg, func(msg maelstrom.Message) error {
		return doSync(s, msg)
	})
}

func doSync(s *state, msg maelstrom.Message) error {
	var body syncResponseMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if !body.Ok || len(body.Messages) < 1 {
		return nil // skip
	}

	s.Lock()
	defer s.Unlock()

	for node, count := range body.Counters {
		s.counters[node].Time = max(s.counters[node].Time, count.Time)
		s.counters[node].LastIndex = max(s.counters[node].LastIndex, count.LastIndex)
	}

	for _, m := range body.Messages {
		s.add(m, msg.Src)
	}

	s.touch()

	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

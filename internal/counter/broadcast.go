package counter

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// HandleWithCommunication uses State based Conflict Free Replicated Data type (at least that's what i think)
// the node id is used as a key and stores the node's view of other nodes' count (as a map).
// there is no need to force consistency as each node would always read it's most recent write (based on sequential consistency)
func HandleWithCommunication(n *maelstrom.Node) {
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	go sync(ctx, n, kv)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body addMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if body.Delta == 0 {
			return n.Reply(msg, map[string]any{"type": "add_ok"})
		}

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		var ok bool

		for i := 0; i < maxRetries && !ok; i++ {
			val, err := readState(n.ID(), ctx, kv)
			if err != nil {
				return err
			}

			valCopy := make(map[string]int, len(val))
			for k, v := range val {
				valCopy[k] = v
			}

			valCopy[n.ID()] += body.Delta

			err = kv.CompareAndSwap(ctx, n.ID(), val, valCopy, true)
			switch {
			case maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed:
				continue // try again

			case err != nil:
				log.Printf("[cas err] %+v", err)
				return err
			}

			ok = true
		}

		if !ok {
			log.Printf("[3] cannot commit add operation")
			return errors.New("cannot commit add operation")
		}

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		val, err := readState(n.ID(), ctx, kv)
		if err != nil {
			return err
		}

		var sum int
		for _, v := range val {
			sum += v
		}

		response := map[string]any{
			"type":  "read_ok",
			"value": sum,
		}

		return n.Reply(msg, response)
	})

	n.Handle("read_only", func(msg maelstrom.Message) error {
		val, err := readState(n.ID(), ctx, kv)
		if err != nil {
			return err
		}

		response := map[string]any{
			"type":  "read_only_ok",
			"value": val[n.ID()],
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func readState(node string, ctx context.Context, kv *maelstrom.KV) (map[string]int, error) {
	var val map[string]int

	err := kv.ReadInto(ctx, node, &val)
	switch {
	case maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist:
		return make(map[string]int), nil

	case err != nil:
		log.Printf("[read err] %+v", err)
		return nil, err
	}

	return val, nil
}

func sync(ctx context.Context, n *maelstrom.Node, kv *maelstrom.KV) {
	t := time.NewTicker(500 * time.Millisecond)
	for range t.C {
		for _, node := range n.NodeIDs() {
			if node == n.ID() {
				continue
			}

			log.Printf("[sync] [%s] starting", node)

			request := map[string]any{"type": "read_only"}

			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			response, err := n.SyncRPC(ctx, node, request)

			if err != nil {
				cancel()
				log.Printf("[sync err] [%s] ask: %+v", node, err)
				continue
			}

			var body map[string]any
			err = json.Unmarshal(response.Body, &body)
			if err != nil {
				cancel()
				log.Printf("[sync err] [%s] unmarshal body: %+v", node, err)
				continue
			}

			var ok bool

			for i := 0; i < maxRetries && !ok; i++ {
				val, err := readState(n.ID(), ctx, kv)
				if err != nil {
					log.Printf("[sync err] [%s] read current state: %+v", node, err)
					continue
				}

				valCopy := make(map[string]int, len(val))
				for k, v := range val {
					valCopy[k] = v
				}

				valCopy[node] = int(body["value"].(float64))

				err = kv.CompareAndSwap(ctx, n.ID(), val, valCopy, true)

				if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
					continue // try again
				} else if err != nil {
					log.Printf("[sync err] [%s] cas: %+v", node, err)
					break
				}

				ok = true
			}

			if !ok {
				log.Printf("[sync err] [%s] cannot commit add operation", node)
			}

			cancel()
		}
	}
}

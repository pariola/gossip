package counter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const maxRetries = 3

type addMsg struct {
	Delta int `json:"delta"`
}

func Handle(n *maelstrom.Node) {
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

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
			val, err := readValue(ctx, kv)
			if err != nil {
				return err
			}

			err = kv.CompareAndSwap(ctx, "key", val, val+body.Delta, true)
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
		forceConsistency(ctx, kv)

		val, err := readValue(ctx, kv)
		if err != nil {
			return err
		}

		response := map[string]any{
			"type":  "read_ok",
			"value": val,
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func readValue(ctx context.Context, kv *maelstrom.KV) (int, error) {
	val, err := kv.ReadInt(ctx, "key")

	switch {
	case maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist:
		return 0, nil

	case err != nil:
		log.Printf("[read err] %+v", err)
		return 0, err
	}

	return val, nil
}

func forceConsistency(ctx context.Context, kv *maelstrom.KV) error {
	// https://github.com/jepsen-io/maelstrom/issues/39#issuecomment-1445414521
	// Constraints of sequential consistency: Just like with a real DB,
	// reads and even identity CAS in seq-kv can be arbitrarily stale; they can be safely reordered to execute in the past.
	//
	// Perform an operation that could not possibly have occurred before the ops you want your client to observe.
	// If it succeeds, any later op your client performs must observe them!
	// This is an implementation detail of maelstrom's data store and not sequentially consistent data stores in general (https://github.com/jepsen-io/maelstrom/issues/39#issuecomment-1476739468)
	now := time.Now().Unix()
	return kv.Write(ctx, fmt.Sprintf("%d", now), now)
}

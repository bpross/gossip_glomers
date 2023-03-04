package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastNode struct {
	n         *maelstrom.Node
	messages  []int
	neighbors []string
}

type BroadcastMessage struct {
	Message int `json:"message"`
}

type TopologyMessage struct {
	Topology map[string][]string
}

func NewBroadcastNode() *BroadcastNode {
	return &BroadcastNode{
		n:         maelstrom.NewNode(),
		messages:  make([]int, 0),
		neighbors: make([]string, 0),
	}
}

func main() {
	bn := NewBroadcastNode()

	bn.n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		body := new(BroadcastMessage)
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}

		bn.messages = append(bn.messages, body.Message)

		for _, neighbor := range bn.neighbors {
			bn.n.Send(neighbor, body)
		}

		resp := make(map[string]string)
		resp["type"] = "broadcast_ok"

		// Echo the original message back with the updated message type.
		return bn.n.Reply(msg, resp)
	})

	bn.n.Handle("read", func(msg maelstrom.Message) error {
		resp := make(map[string]any)
		resp["type"] = "read_ok"
		resp["messages"] = bn.messages

		return bn.n.Reply(msg, resp)
	})

	bn.n.Handle("topology", func(msg maelstrom.Message) error {
		body := new(TopologyMessage)
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := bn.n.ID()
		bn.neighbors = body.Topology[id]

		resp := make(map[string]string)
		resp["type"] = "topology_ok"

		return bn.n.Reply(msg, resp)
	})

	if err := bn.n.Run(); err != nil {
		log.Fatal(err)
	}
}

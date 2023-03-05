package node

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	ADDED = 0
	SEEN  = 1
)

type Node struct {
	n         *maelstrom.Node
	messages  []int
	seen      map[int]struct{}
	neighbors []string
	seenLock  sync.RWMutex
}

type BroadcastMessage struct {
	Message int    `json:"message"`
	Type    string `json:"type"`
}

type TopologyMessage struct {
	Topology map[string][]string
}

func NewBroadcast() *Node {
	return &Node{
		n:         maelstrom.NewNode(),
		messages:  make([]int, 0),
		seen:      make(map[int]struct{}, 0),
		neighbors: make([]string, 0),
	}
}

func (bn *Node) Register() {
	bn.n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := new(BroadcastMessage)
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}

		if val := bn.addNewMessage(body.Message); val == ADDED {
			bn.sendToNeighbors(body.Message)
		}

		resp := make(map[string]string)
		resp["type"] = "broadcast_ok"

		// Echo the original message back with the updated message type.
		return bn.n.Reply(msg, resp)
	})

	bn.n.Handle("internalBroadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		body := new(BroadcastMessage)
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}

		if val := bn.addNewMessage(body.Message); val == ADDED {
			bn.sendToNeighbors(body.Message)
		}

		return nil
	})

	bn.n.Handle("read", func(msg maelstrom.Message) error {
		resp := make(map[string]any)
		resp["type"] = "read_ok"
		bn.seenLock.RLock()
		resp["messages"] = bn.messages
		bn.seenLock.RUnlock()

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
}

func (bn *Node) Run() error {
	return bn.n.Run()
}

func (bn *Node) sendToNeighbors(m int) {
	for _, neighbor := range bn.neighbors {
		msg := &BroadcastMessage{
			Message: m,
			Type:    "internalBroadcast",
		}
		err := bn.n.Send(neighbor, msg)
		if err != nil {
			log.Fatalf("FAILED TO SEND MESSAGE TO: %s %v\n", neighbor, msg)
		}
	}
}

func (bn *Node) addNewMessage(m int) int {
	bn.seenLock.Lock()
	if _, ok := bn.seen[m]; ok {
		return SEEN
	}
	bn.seen[m] = struct{}{}
	bn.messages = append(bn.messages, m)
	bn.seenLock.Unlock()
	return ADDED
}

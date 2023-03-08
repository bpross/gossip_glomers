package node

import (
	"encoding/json"
	"log"
	"sync"
	"time"

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
	Message int                 `json:"message"`
	Type    string              `json:"type"`
	Path    map[string]struct{} `json:"path"`
}

type ReadMessage struct {
	Messages []int  `json:"messages"`
	Type     string `json:"type"`
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

		if body.Path == nil {
			log.Printf("Path was empty")
			body.Path = make(map[string]struct{})
		} else {
			log.Printf("Path: %v\n", body.Path)
		}

		body.Path[msg.Src] = struct{}{}

		if val := bn.addNewMessage(body.Message); val == ADDED {
			//bn.sendToNeighbors(body.Message, body.Path)
		}

		resp := make(map[string]string)
		resp["type"] = "broadcast_ok"

		return bn.n.Reply(msg, resp)
	})

	bn.n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
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

		// Create spanning tree of the topology
		// start with n0, since we know there will always be
		// at least one node
		start := "n0"
		queue := []string{start}
		spanningTree := make(map[string][]string)
		visited := make(map[string]struct{})
		visited[start] = struct{}{}

		for len(queue) > 0 {
			var n string
			n, queue = queue[0], queue[1:]
			for _, neighbor := range body.Topology[n] {
				if _, ok := visited[neighbor]; !ok {
					visited[neighbor] = struct{}{}
					queue = append(queue, neighbor)
					spanningTree[n] = append(spanningTree[n], neighbor)
					spanningTree[neighbor] = append(spanningTree[neighbor], n)
				}
			}
		}

		log.Printf("spanning tree: %v\n", spanningTree)
		bn.neighbors = spanningTree[bn.n.ID()]

		resp := make(map[string]string)
		resp["type"] = "topology_ok"

		return bn.n.Reply(msg, resp)
	})
}

func (bn *Node) Run() error {
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				bn.reconcile()
			}
		}
	}()

	return bn.n.Run()
}

func (bn *Node) sendToNeighbors(m int, path map[string]struct{}) {
	for _, neighbor := range bn.neighbors {
		if _, ok := path[neighbor]; ok {
			continue
		}
		if err := bn.send(neighbor, m, path); err != nil {
			log.Fatalf("FAILED TO SEND MESSAGE TO: %s %d\n", neighbor, m)
		}
	}
}

func (bn *Node) send(neighbor string, message int, path map[string]struct{}) error {
	msg := &BroadcastMessage{
		Message: message,
		Type:    "broadcast",
		Path:    path,
	}
	return bn.n.Send(neighbor, msg)
}

func (bn *Node) addNewMessage(m int) int {
	bn.seenLock.Lock()
	defer bn.seenLock.Unlock()
	if _, ok := bn.seen[m]; ok {
		return SEEN
	}
	bn.seen[m] = struct{}{}
	bn.messages = append(bn.messages, m)
	return ADDED
}

func (bn *Node) readOk(msg maelstrom.Message) error {
	body := new(ReadMessage)
	if err := json.Unmarshal(msg.Body, body); err != nil {
		return err
	}

	neighborSeen := make(map[int]struct{})

	log.Printf("Messages received: %v\n", body.Messages)
	for _, message := range body.Messages {
		neighborSeen[message] = struct{}{}
		if val := bn.addNewMessage(message); val == ADDED {
			log.Printf("message %d added\n", message)
		}
	}

	// 	path := make(map[string]struct{})
	// 	path[bn.n.ID()] = struct{}{}
	// 	for _, message := range bn.messages {
	// 		bn.send(msg.Src, message, path)
	// 	}

	return nil
}

func (bn *Node) reconcile() {
	req := make(map[string]any)
	req["type"] = "read"

	for _, neighbor := range bn.neighbors {
		log.Printf("reconciling with neighbor: %s\n", neighbor)
		err := bn.n.RPC(neighbor, req, bn.readOk)
		if err != nil {
			log.Fatalf("FAILED TO READ FROM NEIGHBOR: %s\n", neighbor)
		}
	}
}

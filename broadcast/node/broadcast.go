package node

import (
	"encoding/json"
	"log"
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
	msgs      chan *internalMessage
}

type internalMessage struct {
	t   string
	msg maelstrom.Message
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
		msgs:      make(chan *internalMessage),
	}
}

func (bn *Node) Register() {
	bn.n.Handle("broadcast", func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   "broadcast",
			msg: msg,
		}
		bn.msgs <- im
		return nil
	})

	bn.n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	bn.n.Handle("read", func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   "read",
			msg: msg,
		}
		bn.msgs <- im
		return nil
	})

	bn.n.Handle("read_ok", func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   "read_ok",
			msg: msg,
		}
		bn.msgs <- im
		return nil
	})

	bn.n.Handle("topology", func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   "topology",
			msg: msg,
		}
		bn.msgs <- im
		return nil
	})
}

func (bn *Node) Run() error {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				bn.reconcile()
			case m := <-bn.msgs:
				switch m.t {
				case "broadcast":
					if err := bn.handleBroadcast(m.msg); err != nil {
						log.Fatalf("error handling broadcast")
					}
				case "read":
					if err := bn.handleRead(m.msg); err != nil {
						log.Fatalf("error handling read")
					}
				case "read_ok":
					if err := bn.readOk(m.msg); err != nil {
						log.Fatalf("error handling read")
					}
				case "topology":
					if err := bn.handleTopology(m.msg); err != nil {
						log.Fatalf("error handling topology")
					}
				}
			default:
			}
		}
	}()

	return bn.n.Run()
}
func (bn *Node) handleBroadcast(msg maelstrom.Message) error {
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
}

func (bn *Node) handleRead(msg maelstrom.Message) error {
	resp := make(map[string]any)
	resp["type"] = "read_ok"
	resp["messages"] = bn.messages

	return bn.n.Reply(msg, resp)
}

func (bn *Node) handleTopology(msg maelstrom.Message) error {
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

	for _, message := range body.Messages {
		bn.addNewMessage(message)
	}

	return nil
}

func (bn *Node) reconcile() {
	req := make(map[string]any)
	req["type"] = "read"

	for _, neighbor := range bn.neighbors {
		err := bn.n.Send(neighbor, req)
		if err != nil {
			log.Fatalf("FAILED TO READ FROM NEIGHBOR: %s\n", neighbor)
		}
	}
}

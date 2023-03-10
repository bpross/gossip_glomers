package node

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/meirf/gopart"
)

const (
	ADDED     = 0
	SEEN      = 1
	BROADCAST = "broadcast"
	INTERNAL  = "internalBroadcast"
)

type Node struct {
	n            *maelstrom.Node
	messages     []int
	seen         map[int]struct{}
	neighbors    []string
	msgs         chan *internalMessage
	partitions   [][]string
	partitionIdx int
}

type internalMessage struct {
	t   string
	msg maelstrom.Message
}

type BroadcastMessage struct {
	Message   int                 `json:"message"`
	Type      string              `json:"type"`
	Path      map[string]struct{} `json:"path"`
	Propegate bool                `json:"propegate"`
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
		n:          maelstrom.NewNode(),
		messages:   make([]int, 0),
		seen:       make(map[int]struct{}, 0),
		neighbors:  make([]string, 0),
		msgs:       make(chan *internalMessage),
		partitions: make([][]string, 0),
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

	bn.n.Handle("internalBroadcast", func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   "internalBroadcast",
			msg: msg,
		}
		bn.msgs <- im
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
	rand.Seed(time.Now().UnixNano())
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				bn.reconcile()
			default:
			}
		}
	}()

	go func() {
		for {
			select {
			case m := <-bn.msgs:
				switch m.t {
				case "broadcast":
					if err := bn.handleBroadcast(m.msg); err != nil {
						log.Fatalf("error handling broadcast")
					}
				case "internalBroadcast":
					bn.handleInternalBroadcast(m.msg)
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

	bn.sendToNeighbors(body.Message, body.Path)

	// rn := bn.getRandomNeighbor()
	// if rn != "" {
	// 	body.Propegate = true
	// 	bn.send(INTERNAL, rn, body.Message, body.Path)
	// }

	resp := make(map[string]string)
	resp["type"] = "broadcast_ok"

	return bn.n.Reply(msg, resp)
}

func (bn *Node) handleInternalBroadcast(msg maelstrom.Message) error {
	body := new(BroadcastMessage)
	if err := json.Unmarshal(msg.Body, body); err != nil {
		return err
	}

	bn.addNewMessage(body.Message)

	if body.Propegate {
		bn.sendToNeighbors(body.Message, body.Path)
	}

	return nil
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

	nodes := make([]string, len(body.Topology))
	i := 0
	idx := 0
	for node := range body.Topology {
		if node == bn.n.ID() {
			idx = i
		}
		nodes[i] = node
		i++
	}

	partitionSize := len(nodes)/2 + 1
	for idxRange := range gopart.Partition(len(nodes), partitionSize) {
		partition := make([]string, partitionSize)
		partition = nodes[idxRange.Low:idxRange.High]
		bn.partitions = append(bn.partitions, partition)
		if idxRange.Low <= idx && idx <= idxRange.High {
			bn.partitionIdx = len(bn.partitions) - 1
		}
	}

	bn.neighbors = bn.partitions[bn.partitionIdx]

	resp := make(map[string]string)
	resp["type"] = "topology_ok"

	return bn.n.Reply(msg, resp)
}
func (bn *Node) sendToNeighbors(m int, path map[string]struct{}) {
	for _, neighbor := range bn.n.NodeIDs() {
		if _, ok := path[neighbor]; ok {
			continue
		}
		if err := bn.send(INTERNAL, neighbor, m, path); err != nil {
			log.Fatalf("FAILED TO SEND MESSAGE TO: %s %d\n", neighbor, m)
		}
	}
}

func (bn *Node) send(t, neighbor string, message int, path map[string]struct{}) error {
	msg := &BroadcastMessage{
		Message: message,
		Type:    t,
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
		if val := bn.addNewMessage(message); val == ADDED {
			for _, neighbor := range bn.neighbors {
				path := make(map[string]struct{})
				path[bn.n.ID()] = struct{}{}
				bn.send(INTERNAL, neighbor, message, path)
			}
		}
	}

	return nil
}

func (bn *Node) reconcile() {
	req := make(map[string]any)
	req["type"] = "read"

	neighbor := bn.getRandomNeighbor()
	if neighbor == "" {
		return
	}
	err := bn.n.Send(neighbor, req)
	if err != nil {
		log.Fatalf("FAILED TO READ FROM NEIGHBOR: %s\n", neighbor)
	}
}

func (bn *Node) getRandomNeighbor() string {
	idx := bn.partitionIdx

	r := len(bn.partitions)
	if r == 0 {
		return ""
	}
	for idx == bn.partitionIdx {
		idx = rand.Intn(r)
	}

	return bn.partitions[idx][0]
}

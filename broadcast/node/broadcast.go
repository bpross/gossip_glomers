package node

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// ADDED Message has been added to the node
	ADDED = 0

	// SEEN Message has already been seen by this node
	SEEN = 1
)

// Node wrapper to stare local data and state
type Node struct {
	n         *maelstrom.Node
	messages  []int
	seen      map[int]struct{}
	neighbors []string
	msgs      chan *internalMessage
	idx       int
}

// NewBroadcast returns a new Node
func NewBroadcast() *Node {
	return &Node{
		n:         maelstrom.NewNode(),
		messages:  make([]int, 0),
		seen:      make(map[int]struct{}, 0),
		neighbors: make([]string, 0),
		msgs:      make(chan *internalMessage),
	}
}

// Register Handlers to handle the appropriate messages
// from the test bench and other nodes
// MUST BE CALLED BEFORE RUN
func (bn *Node) Register() {
	bn.n.Handle(BROADCAST, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   BROADCAST,
			msg: msg,
		}
		bn.msgs <- im
		return nil
	})

	bn.n.Handle(INTERNAL, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   INTERNAL,
			msg: msg,
		}
		bn.msgs <- im
		return nil
	})

	bn.n.Handle(READ, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   READ,
			msg: msg,
		}
		bn.msgs <- im
		return nil
	})

	bn.n.Handle(READOK, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   READOK,
			msg: msg,
		}
		bn.msgs <- im
		return nil
	})

	bn.n.Handle(TOPOLOGY, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   TOPOLOGY,
			msg: msg,
		}
		bn.msgs <- im
		return nil
	})
}

// Run Setups the event loop and the reconcile loop
// Runs the node
func (bn *Node) Run() error {
	rand.Seed(time.Now().UnixNano())

	go bn.timedReconcile()
	go bn.eventLoop()

	return bn.n.Run()
}

// Function to reconcile with other nodes
// in the cluster
func (bn *Node) timedReconcile() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			bn.reconcile()
		default:
		}
	}
}

// Loop to handle all incoming events
// using channels here allows us to avoid
// using mutexes
func (bn *Node) eventLoop() {
	for {
		select {
		case m := <-bn.msgs:
			switch m.t {
			case BROADCAST:
				if err := bn.handleBroadcast(m.msg); err != nil {
					log.Fatalf("error handling broadcast")
				}
			case INTERNAL:
				bn.handleInternalBroadcast(m.msg)
			case READ:
				if err := bn.handleRead(m.msg); err != nil {
					log.Fatalf("error handling read")
				}
			case READOK:
				if err := bn.readOk(m.msg); err != nil {
					log.Fatalf("error handling read")
				}
			case TOPOLOGY:
				if err := bn.handleTopology(m.msg); err != nil {
					log.Fatalf("error handling topology")
				}
			}
		default:
		}
	}
}

// Handle broadcast message from a client and send the message
// to all of the nodes neighbors
func (bn *Node) handleBroadcast(msg maelstrom.Message) error {
	body := new(BroadcastMessage)
	if err := json.Unmarshal(msg.Body, body); err != nil {
		return err
	}

	bn.sendToNeighbors(body.Message)

	resp := make(map[string]string)
	resp["type"] = BROADCASTOK

	return bn.n.Reply(msg, resp)
}

// handle broadcast message from another node
// Does not send to other nodes, nor does it reply
// removing the reply reduces message ops
func (bn *Node) handleInternalBroadcast(msg maelstrom.Message) error {
	body := new(BroadcastMessage)
	if err := json.Unmarshal(msg.Body, body); err != nil {
		return err
	}

	bn.addNewMessage(body.Message)

	return nil
}

// Handles incoming read request from client or other node
func (bn *Node) handleRead(msg maelstrom.Message) error {
	resp := make(map[string]any)
	resp["type"] = READOK
	resp["messages"] = bn.messages

	return bn.n.Reply(msg, resp)
}

// Handles incoming topology messages
func (bn *Node) handleTopology(msg maelstrom.Message) error {
	body := new(TopologyMessage)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	nodes := make([]string, len(body.Topology))
	i := 0
	bn.idx = 0
	for node := range body.Topology {
		if node == bn.n.ID() {
			bn.idx = i
		}
		nodes[i] = node
		i++
	}

	bn.neighbors = nodes

	resp := make(map[string]string)
	resp["type"] = TOPOLOGYOK

	return bn.n.Reply(msg, resp)
}

// Sends the provided message to all neighbors
func (bn *Node) sendToNeighbors(m int) {
	for _, neighbor := range bn.neighbors {
		if err := bn.send(INTERNAL, neighbor, m); err != nil {
			log.Fatalf("FAILED TO SEND MESSAGE TO: %s %d\n", neighbor, m)
		}
	}
}

// Wrapper around maelstrom node send
func (bn *Node) send(t, neighbor string, message int) error {
	msg := &BroadcastMessage{
		Message: message,
		Type:    t,
	}
	return bn.n.Send(neighbor, msg)
}

// Adds a new message to the node
// Use seen to avoid duplicates in the response
func (bn *Node) addNewMessage(m int) int {
	if _, ok := bn.seen[m]; ok {
		return SEEN
	}
	bn.seen[m] = struct{}{}
	bn.messages = append(bn.messages, m)
	return ADDED
}

// Handles a read response from another node
// Any message that this node has not seen will
// be sent to all other nodes as a fail safe
// to improve consistency
func (bn *Node) readOk(msg maelstrom.Message) error {
	body := new(ReadMessage)
	if err := json.Unmarshal(msg.Body, body); err != nil {
		return err
	}

	for _, message := range body.Messages {
		if val := bn.addNewMessage(message); val == ADDED {
			bn.sendToNeighbors(message)
		}
	}

	return nil
}

// Picks a random neighbor and asks
// them for the state of their world
// this will respond will read_ok
func (bn *Node) reconcile() {
	req := make(map[string]any)
	req["type"] = READ

	neighbor := bn.getRandomNeighbor()
	if neighbor == "" {
		return
	}
	err := bn.n.Send(neighbor, req)
	if err != nil {
		log.Fatalf("FAILED TO READ FROM NEIGHBOR: %s\n", neighbor)
	}
}

// Picks a random neighbor
func (bn *Node) getRandomNeighbor() string {
	idx := bn.idx

	r := len(bn.neighbors)
	if r == 0 {
		return ""
	}

	for idx == bn.idx {
		idx = rand.Intn(r)
	}

	return bn.neighbors[idx]
}

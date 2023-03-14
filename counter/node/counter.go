package node

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// KEY is the key used to store the counter in the KV store
	KEY = "counter"
)

// Node is a wrapper around the maelstrom.Node
// that stores state regarding the local counter
type Node struct {
	n    *maelstrom.Node
	kv   *maelstrom.KV
	msgs chan *internalMessage

	mu sync.Mutex

	value      int
	operations []int
}

// NewCounter creates a new node
func NewCounter() *Node {
	n := &Node{
		n:          maelstrom.NewNode(),
		msgs:       make(chan *internalMessage),
		operations: make([]int, 0),
	}
	n.kv = maelstrom.NewSeqKV(n.n)

	return n
}

// Register setups the handler functions to write
// to the event loop
func (cn *Node) Register() {
	cn.n.Handle(ADD, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   ADD,
			msg: msg,
		}

		cn.msgs <- im
		return nil
	})

	cn.n.Handle(READ, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   READ,
			msg: msg,
		}

		cn.msgs <- im
		return nil
	})
}

// Run starts the event loop, timed flush
// and the node
func (cn *Node) Run() error {
	go cn.eventLoop()
	go cn.timedFlush()
	return cn.n.Run()
}

// Handles events as they come across the channel
// this might not be as useful, since we do need
// a mutex to handle writing to the KV store
func (cn *Node) eventLoop() {
	for {
		select {
		case m := <-cn.msgs:
			switch m.t {
			case READ:
				if err := cn.handleRead(m.msg); err != nil {
					log.Fatalf("error handling read")
				}
			case ADD:
				if err := cn.handleAdd(m.msg); err != nil {
					log.Fatalf("error handling add")
				}
			}
		default:
		}
	}
}

// handles a read request from a client
// returns the locally stored value
func (cn *Node) handleRead(m maelstrom.Message) error {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	resp := make(map[string]any)
	resp["type"] = READOK
	resp["value"] = cn.value

	return cn.n.Reply(m, resp)
}

// handles an add request from a client
// modifies the locally stored value
func (cn *Node) handleAdd(m maelstrom.Message) error {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	body := new(AddRequest)
	if err := json.Unmarshal(m.Body, body); err != nil {
		return err
	}

	cn.value += body.Delta
	cn.operations = append(cn.operations, body.Delta)

	resp := make(map[string]any)
	resp["type"] = ADDOK

	return cn.n.Reply(m, resp)
}

// flushes all operations to the KV store
// at an interval
// If flush fails to update, could be worth
// randomizing this to avoid contention
// with other nodes
func (cn *Node) timedFlush() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			cn.flush()
		default:
		}
	}
}

// first tries to read the value from the KV store
// creates the value if its not defined
// runs the operations on the value thats stored in
// the KV store and tries to write the value
// if the value ha changed while this flush has been running
// just fail and try again later
// Set the current value to the new value thats stored in the KV
// and clear all operations
func (cn *Node) flush() {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	ctx := context.Background()

	initialValue, err := cn.kv.ReadInt(ctx, KEY)
	if err != nil {
		var rpcError *maelstrom.RPCError
		if errors.As(err, &rpcError) {
			if rpcError.Code == maelstrom.KeyDoesNotExist {
				err = cn.kv.CompareAndSwap(ctx, KEY, 0, 0, true)
				if err != nil {
					log.Printf("COULD NOT CREATE VALUE: %v\n", err)
					return
				}
			} else {
				log.Printf("COULD NOT GET VALUE FROM KV: %v\n", err)
				return
			}
		} else {
			log.Printf("COULD NOT GET VALUE FROM KV: %v\n", err)
			return
		}
	}

	newValue := initialValue
	for _, op := range cn.operations {
		newValue += op
	}

	// Use this as a "lock", only update if the value hasn't changed from
	// under us
	err = cn.kv.CompareAndSwap(ctx, KEY, initialValue, newValue, false)
	if err != nil {
		log.Printf("COULD NOT CAS VALUE: %v\n", err)
		return
	}

	cn.operations = make([]int, 0)
	cn.value = newValue
	log.Printf("SUCCESSFULLY FLUSHED OPERATIONS")
}

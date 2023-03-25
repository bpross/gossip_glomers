package node

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Node is a wrapper around the maelstrom.Node
// that stores state regarding local kafka storage
type Node struct {
	n    *maelstrom.Node
	kv   *maelstrom.KV
	msgs chan *internalMessage

	committedOffsets map[string]int
	casErrors        int
}

// NewKafka creates a new node that implements
// the kafka protocol
func NewKafka() *Node {
	n := &Node{
		n:                maelstrom.NewNode(),
		msgs:             make(chan *internalMessage),
		committedOffsets: make(map[string]int),
	}

	n.kv = maelstrom.NewLinKV(n.n)
	return n
}

// Register registers all handler funcs for the necessary
// messages
func (kn *Node) Register() {
	kn.n.Handle(SEND, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   SEND,
			msg: msg,
		}
		kn.msgs <- im
		return nil
	})
	kn.n.Handle(POLL, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   POLL,
			msg: msg,
		}
		kn.msgs <- im
		return nil
	})
	kn.n.Handle(LISTCOMMITTEDOFFSETS, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   LISTCOMMITTEDOFFSETS,
			msg: msg,
		}
		kn.msgs <- im
		return nil
	})
	kn.n.Handle(COMMITOFFSETS, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   COMMITOFFSETS,
			msg: msg,
		}
		kn.msgs <- im
		return nil
	})
}

// Run setups the event loop and starts the node
func (kn *Node) Run() error {
	go kn.eventLoop()
	return kn.n.Run()
}

// event loop handles incoming messages
// and passes them off to the appropriate handlers
func (kn *Node) eventLoop() {
	for {
		select {
		case m := <-kn.msgs:
			switch m.t {
			case COMMITOFFSETS:
				if err := kn.handleCommitOffsets(m.msg); err != nil {
					log.Fatalf("error handling commit offsets")
				}
			case LISTCOMMITTEDOFFSETS:
				if err := kn.handleListCommittedOffsets(m.msg); err != nil {
					log.Fatalf("error handling list committed offsets")
				}
			case POLL:
				if err := kn.handlePoll(m.msg); err != nil {
					log.Fatalf("error handling poll")
				}
			case SEND:
				if err := kn.handleSend(m.msg); err != nil {
					log.Fatalf("error handling send")
				}
			}
		default:
		}
	}
}

// handleSend handles a send message from a client
// it ALWAYS writes to the KV store until it was successful
func (kn *Node) handleSend(m maelstrom.Message) error {
	log.Printf("handling send\n")
	body := new(SendRequest)
	if err := json.Unmarshal(m.Body, body); err != nil {
		return err
	}

	written := false
	var offset int
	var err error
	for !written {
		offset, err = kn.writeToKV(body.Key, body.Msg)
		if err != nil {
			var rpcError *maelstrom.RPCError
			if errors.As(err, &rpcError) {
				if rpcError.Code != maelstrom.PreconditionFailed {
					log.Fatalf("COULD NOT WRITE TO KV: %v\n", err)
					return err
				}
				kn.casErrors++
				log.Printf("number of CAS errors: %d\n", kn.casErrors)
			}
		} else {
			written = true
		}
	}

	resp := &SendResponse{
		Type:   SENDOK,
		Offset: offset,
	}

	log.Printf("SEND REPLYING: %v\n", resp)
	return kn.n.Reply(m, resp)
}

// handlePoll handles a poll message from the client
// it ALWAYS reads from the KV store
func (kn *Node) handlePoll(m maelstrom.Message) error {
	log.Printf("handling poll\n")
	body := new(PollRequest)
	if err := json.Unmarshal(m.Body, body); err != nil {
		return err
	}

	resp := &PollResponse{
		Type: POLLOK,
		Msgs: make(map[string][][]int),
	}

	for k, offset := range body.Offsets {
		// just give them the rest of the messages
		msgs := make([][]int, 0)
		i := offset
		storedMsgs, err := kn.readFromKV(k, false)
		if err != nil {
			var rpcError *maelstrom.RPCError
			if errors.As(err, &rpcError) {
				if rpcError.Code != maelstrom.KeyDoesNotExist {
					log.Printf("COULD NOT GET VALUE FROM KV: %v\n", err)
					return err
				}
			}
		} else {
			for i < len(storedMsgs) {
				val, ok := storedMsgs[i].(float64)
				if !ok {
					log.Fatalf("VALUE FROM KV NOT INT: %T %v\n", storedMsgs[i], storedMsgs[i])
				}
				msgs = append(msgs, []int{i, int(val)})
				i++
			}
		}
		resp.Msgs[k] = msgs
	}

	log.Printf("POLL REPLYING: %v\n", resp)
	return kn.n.Reply(m, resp)
}

// handles a client committing its offsets
func (kn *Node) handleCommitOffsets(m maelstrom.Message) error {
	log.Printf("handling commit offset\n")
	body := new(CommitOffsetsRequest)
	if err := json.Unmarshal(m.Body, body); err != nil {
		return err
	}

	for k, offset := range body.Offsets {
		kn.committedOffsets[k] = offset
	}

	resp := make(map[string]any)
	resp["type"] = COMMITOFFSETOK

	log.Printf("CO replying %v\n", resp)
	return kn.n.Reply(m, resp)
}

// handles returning offsets that have been committed
func (kn *Node) handleListCommittedOffsets(m maelstrom.Message) error {
	log.Printf("list committed offsets\n")
	body := new(ListCommitOffsetsRequest)
	if err := json.Unmarshal(m.Body, body); err != nil {
		return err
	}

	resp := &ListCommittedOffsetsResponse{
		Type:    LISTCOMMITTEEDOFFSETSOK,
		Offsets: make(map[string]int),
	}

	for _, k := range body.Keys {
		resp.Offsets[k] = kn.committedOffsets[k]
	}

	log.Printf("LCO replying: %v\n", resp)
	return kn.n.Reply(m, resp)
}

// reads from the underlying kv store
func (kn *Node) readFromKV(key string, create bool) ([]interface{}, error) {
	ctx := context.Background()
	var initialValue interface{}

	initialValue, err := kn.kv.Read(ctx, key)
	if err != nil {
		var rpcError *maelstrom.RPCError
		if errors.As(err, &rpcError) {
			if rpcError.Code == maelstrom.KeyDoesNotExist && create {
				err = kn.kv.CompareAndSwap(ctx, key, []interface{}{}, []interface{}{}, true)
				if err != nil {
					log.Printf("COULD NOT CREATE VALUE: %v\n", err)
					return nil, err
				}
			} else {
				log.Printf("COULD NOT GET VALUE FROM KV: %v\n", err)
				return nil, err
			}
		} else {
			log.Printf("COULD NOT GET VALUE FROM KV: %v\n", err)
			return nil, err
		}
	}

	if initialValue == nil {
		return []interface{}{}, nil
	}

	val, ok := initialValue.([]interface{})
	if !ok {
		log.Printf("BAD VAL FOR KEY: %T %v\n", initialValue, initialValue)
	}
	return val, nil
}

// writes to the underlying kv store
func (kn *Node) writeToKV(key string, val int) (int, error) {
	initialValue, err := kn.readFromKV(key, true)
	if err != nil {
		return -1, err
	}

	log.Printf("before writing: %v\n", initialValue)
	ctx := context.Background()
	err = kn.kv.CompareAndSwap(ctx, key, initialValue, append(initialValue, val), true)
	return len(initialValue), err
}

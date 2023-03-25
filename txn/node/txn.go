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
}

func NewTxn() *Node {
	n := &Node{
		n:    maelstrom.NewNode(),
		msgs: make(chan *internalMessage),
	}
	n.kv = maelstrom.NewSeqKV(n.n)

	return n
}

func (tn *Node) Register() {
	tn.n.Handle(TXN, func(msg maelstrom.Message) error {
		im := &internalMessage{
			t:   TXN,
			msg: msg,
		}
		tn.msgs <- im
		return nil
	})
}

func (tn *Node) Run() error {
	go tn.eventLoop()
	return tn.n.Run()
}

func (tn *Node) eventLoop() {
	for {
		select {
		case m := <-tn.msgs:
			switch m.t {
			case TXN:
				if err := tn.handleTxn(m.msg); err != nil {
					log.Fatalf("error handling txn")
				}
			}
		default:
		}
	}
}

func (tn *Node) handleTxn(m maelstrom.Message) error {
	body := new(TxnRequest)
	if err := json.Unmarshal(m.Body, body); err != nil {
		return err
	}

	results := make([][]interface{}, 0)
	ctx := context.Background()

	tx := make(map[int]int)

	for _, operation := range body.Transactions {
		if len(operation) != 3 {
			log.Fatalf("INVALID OPERATION FROM CLIENT\n")
			return nil
		}

		op, ok := operation[0].(string)
		if !ok {
			log.Fatalf("OPERATION IS NOT STRING: %T\n", op)
			return nil
		}

		key := interfaceToInt(operation[1])

		if op == "w" {
			val := interfaceToInt(operation[2])
			if !ok {
				log.Fatalf("VALUE IS NOT INT: %T\n", val)
			}

			tx[key] = val

			results = append(results, operation)
		} else {
			val, err := tn.readFromKV(string(key))
			if err != nil {
				log.Fatalf("could not get value from kv: %v\n", err)
			}
			results = append(results, []interface{}{op, key, val})
		}
	}

	for key, val := range tx {
		if err := tn.kv.Write(ctx, string(key), val); err != nil {
			log.Fatalf("COULD NOT WRITE: %d, %d: %v\n", key, val, err)
		}
	}

	resp := &TxnResponse{
		Type:         TXNOK,
		Transactions: results,
	}

	return tn.n.Reply(m, resp)
}

// reads from the underlying kv store
func (tn *Node) readFromKV(key string) (interface{}, error) {
	ctx := context.Background()
	var initialValue interface{}

	initialValue, err := tn.kv.Read(ctx, key)
	if err != nil {
		var rpcError *maelstrom.RPCError
		if errors.As(err, &rpcError) {
			if rpcError.Code == maelstrom.KeyDoesNotExist {
				return nil, nil
			} else {
				log.Printf("COULD NOT GET VALUE FROM KV: %v\n", err)
				return nil, err
			}
		} else {
			log.Printf("COULD NOT GET VALUE FROM KV: %v\n", err)
			return nil, err
		}
	}

	return initialValue, nil
}

func interfaceToInt(val interface{}) int {
	var i int
	switch t := val.(type) {
	case int:
		i = t
	case int8:
		i = int(t) // standardizes across systems
	case int16:
		i = int(t) // standardizes across systems
	case int32:
		i = int(t) // standardizes across systems
	case int64:
		i = int(t) // standardizes across systems
	case bool:
		log.Fatalf("NOT CONVERTABLE TYPE BOOL\n")
	case float32:
		i = int(t) // standardizes across systems
	case float64:
		i = int(t) // standardizes across systems
	case uint8:
		i = int(t) // standardizes across systems
	case uint16:
		i = int(t) // standardizes across systems
	case uint32:
		i = int(t) // standardizes across systems
	case uint64:
		i = int(t) // standardizes across systems
	case string:
		log.Fatalf("NOT CONVERTABLE TYPE STRING\n")
	default:
		// what is it then?
		log.Fatalf("%v == %T\n", t, t)
	}

	return i
}

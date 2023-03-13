package node

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	n     *maelstrom.Node
	kv    *maelstrom.KV
	msgs  chan *internalMessage
	value int
}

func NewCounter() *Node {
	n := &Node{
		n:    maelstrom.NewNode(),
		msgs: make(chan *internalMessage),
	}
	n.kv = maelstrom.NewSeqKV(n.n)

	return n
}

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

func (cn *Node) Run() error {
	go cn.eventLoop()
	return cn.n.Run()
}

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

func (cn *Node) handleRead(m maelstrom.Message) error {
	resp := make(map[string]any)
	resp["type"] = READOK
	resp["value"] = cn.value

	return cn.n.Reply(m, resp)
}

func (cn *Node) handleAdd(m maelstrom.Message) error {
	body := new(AddRequest)
	if err := json.Unmarshal(m.Body, body); err != nil {
		return err
	}

	cn.value += body.Delta

	resp := make(map[string]any)
	resp["type"] = ADDOK

	return cn.n.Reply(m, resp)
}

package node

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Node is a wrapper around the maelstrom.Node
// that stores state regarding local kafka storage
type Node struct {
	n    *maelstrom.Node
	msgs chan *internalMessage

	committedOffsets map[string]int
	storedMessages   map[string][]int
}

func NewKafka() *Node {
	return &Node{
		n:                maelstrom.NewNode(),
		msgs:             make(chan *internalMessage),
		committedOffsets: make(map[string]int),
		storedMessages:   make(map[string][]int),
	}
}

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

func (kn *Node) Run() error {
	go kn.eventLoop()
	return kn.n.Run()
}

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

func (kn *Node) handleSend(m maelstrom.Message) error {
	log.Printf("handling send\n")
	body := new(SendRequest)
	if err := json.Unmarshal(m.Body, body); err != nil {
		return err
	}

	if val, ok := kn.storedMessages[body.Key]; ok {
		val = append(val, body.Msg)
		kn.storedMessages[body.Key] = val
	} else {
		kn.storedMessages[body.Key] = []int{body.Msg}
	}

	resp := &SendResponse{
		Type:   SENDOK,
		Offset: len(kn.storedMessages[body.Key]) - 1,
	}

	log.Printf("SEND REPLYING: %v\n", resp)
	return kn.n.Reply(m, resp)
}

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
		storedMsgs := kn.storedMessages[k]
		for i < len(kn.storedMessages[k]) {
			msgs = append(msgs, []int{i, storedMsgs[i]})
			i++
		}
		resp.Msgs[k] = msgs
	}

	log.Printf("POLL REPLYING: %v\n", resp)
	return kn.n.Reply(m, resp)
}

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

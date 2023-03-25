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
	msgs chan *internalMessage

	kvStore map[int]int
}

func NewNode() *Node {

}

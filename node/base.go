package node

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	n    *maelstrom.Node
	msgs chan *internalMessage
}

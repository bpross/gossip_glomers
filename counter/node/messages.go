package node

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Requests
	READ = "read"
	ADD  = "add"

	// Responses
	ADDOK  = "add_ok"
	READOK = "read_ok"
)

type AddRequest struct {
	Delta int `json:"delta"`
}

// Wrap the library message so we can include type
// then we dont have to unmarshal more than once
type internalMessage struct {
	t   string
	msg maelstrom.Message
}

type ReadResponse struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

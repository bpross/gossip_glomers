package node

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Requests

	// READ is the slug for a read request from a client
	READ = "read"
	// ADD is the slug for an add request from a client
	ADD = "add"

	// Responses

	// ADDOK is the slug for an add response
	ADDOK = "add_ok"
	// READOK is the slug for a read response
	READOK = "read_ok"
)

// AddRequest represents the important information included
// in an Add request from the client
type AddRequest struct {
	Delta int `json:"delta"`
}

// Wrap the library message so we can include type
// then we dont have to unmarshal more than once
type internalMessage struct {
	t   string
	msg maelstrom.Message
}

// ReadResponse represents the important information
// included in a read response thats sent to the client
type ReadResponse struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

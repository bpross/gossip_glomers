package node

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// REQUESTS

	// BROADCAST is the slug for the broadcast message
	BROADCAST = "broadcast"
	// INTERNAL is the slug for internal broadcast message
	INTERNAL = "internalBroadcast"
	// READ is the slug for the read message
	READ = "read"
	// TOPOLOGY is the slug for the topology message
	TOPOLOGY = "topology"

	// RESPONSES

	// BROADCASTOK is the slug for the broadcast response messsage
	BROADCASTOK = "broadcast_ok"
	// READOK is the slug for the read response message
	READOK = "read_ok"
	// TOPOLOGYOK is the slug for the topology response message
	TOPOLOGYOK = "topology_ok"
)

// Wrap the library message so we can include type
// then we dont have to unmarshal more than once
type internalMessage struct {
	t   string
	msg maelstrom.Message
}

// BroadcastMessage received from the test bench
type BroadcastMessage struct {
	Message int    `json:"message"`
	Type    string `json:"type"`
}

// ReadMessage contains the messages
// seen by the node
type ReadMessage struct {
	Messages []int  `json:"messages"`
	Type     string `json:"type"`
}

// TopologyMessage contains network topology
// information for the nodes
type TopologyMessage struct {
	Topology map[string][]string
}

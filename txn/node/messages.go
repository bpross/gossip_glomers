package node

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	TXN = "txn"

	TXNOK = "txn_ok"
)

type TxnRequest struct {
	Type         string          `json:"type"`
	Transactions [][]interface{} `json:"txn"`
}

type TxnResponse struct {
	Type         string          `json:"type"`
	Transactions [][]interface{} `json:"txn"`
}

// Wrap the library message so we can include type
// then we dont have to unmarshal more than once
type internalMessage struct {
	t   string
	msg maelstrom.Message
}

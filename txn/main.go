package main

import (
	"log"

	"github.com/bpross/gossip_glomers/txn/node"
)

func main() {
	bn := node.NewTxn()

	bn.Register()

	if err := bn.Run(); err != nil {
		log.Fatal(err)
	}
}

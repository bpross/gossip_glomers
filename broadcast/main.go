package main

import (
	"log"

	"github.com/bpross/gossip_glomers/broadcast/node"
)

func main() {
	bn := node.NewBroadcast()

	bn.Register()

	if err := bn.Run(); err != nil {
		log.Fatal(err)
	}
}

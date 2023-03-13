package main

import (
	"log"

	"github.com/bpross/gossip_glomers/counter/node"
)

func main() {
	bn := node.NewCounter()

	bn.Register()

	if err := bn.Run(); err != nil {
		log.Fatal(err)
	}
}

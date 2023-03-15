package main

import (
	"log"

	"github.com/bpross/gossip_glomers/kafka/node"
)

func main() {
	bn := node.NewKafka()

	bn.Register()

	if err := bn.Run(); err != nil {
		log.Fatal(err)
	}
}

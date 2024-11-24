package main

import (
	"github.com/alxibra/queue-splitter/servers"
)

func main() {
	s := servers.Init()
	defer s.Close()
	s.Run()
}

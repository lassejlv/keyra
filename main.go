package main

import (
	"log"
	"redis-go-clone/server"
)

func main() {
	srv := server.New(":6379")
	log.Fatal(srv.Start())
}

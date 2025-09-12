package main

import (
	"keyra/server"
	"log"
)

func main() {
	srv := server.New(":6379")
	log.Fatal(srv.Start())

}

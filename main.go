package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"trade-caddie/tradeclient"

	"google.golang.org/grpc"
)

func main() {
	// create grpc connection
	conn, err := grpc.Dial(":5000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error connecting to server: %v", err)
	}
	defer conn.Close()

	client := tradeclient.NewTradeClient(conn)
	defer client.Disconnect()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT)
	signal.Notify(done, syscall.SIGTERM)

	<-done
}

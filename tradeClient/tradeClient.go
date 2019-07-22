package main

import (
	"flag"
	"log"
	"os"
	"time"
	"trade-caddie/tradepb"

	"google.golang.org/grpc"
)

var client tradepb.TradeServiceClient
var logger *log.Logger
var port *string

func init() {
	// initialize command line flags
	port = flag.String("port", ":5000", "port server is running on")
	flag.Parse()

	// initialize logger
	f, err := os.OpenFile("log.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("Unable to open log file")
	}
	defer f.Close()

	logger = log.New(f, time.Now().Format("01-02-2006 15:04:05"), 0)
	logger.Printf("Starting client")
}

func main() {
	// create grpc connection
	conn, err := grpc.Dial(*port, grpc.WithInsecure())
	if err != nil {
		logger.Fatalf("Error connecting to server: %v", err)
	}
	defer conn.Close()

	// initialize client
	client = tradepb.NewTradeServiceClient(conn)
}

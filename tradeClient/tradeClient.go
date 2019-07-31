package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"
	"trade-caddie/tradepb"

	"google.golang.org/grpc"
)

var client tradepb.TradeServiceClient
var logger *log.Logger
var logFile *os.File
var port *string

func init() {
	// initialize command line flags
	port = flag.String("port", ":5000", "port server is running on")
	flag.Parse()

	// initialize logger
	logFile, err := os.OpenFile("tradeClient/log.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Unable to open log file")
	}

	logger = log.New(logFile, time.Now().Format("01-02-2006 15:04:05 "), 0)
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

	trade, err := GetTrade("5d38f04871d3c9d51a8f299a", 1, client)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(rowify(trade))

	// channel to receive interrupt command
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT)
	signal.Notify(stopChan, syscall.SIGTERM)

	// cleanup resources on interrupt
	go func() {
		sig := <-stopChan
		logger.Printf("signal: %+v received. Shutting down", sig)
		defer logFile.Close()
		os.Exit(0)
	}()

	<-stopChan
}

// AddTrade adds a trade to a portfolio
func AddTrade(trade *tradepb.Trade, portfolioID int32, client tradepb.TradeServiceClient) (string, error) {
	req := &tradepb.AddTradeRequest{
		Trade:       trade,
		PortfolioId: portfolioID,
	}

	res, err := client.AddTrade(context.Background(), req)
	if err != nil {
		logger.Printf("Error calling AddTrade: %v", err)
		return "", err
	}

	return res.GetTradeId(), nil
}

// DeleteTrade deletes a trade from the database given an _id
func DeleteTrade(tradeID string, portfolioID int32, client tradepb.TradeServiceClient) error {
	req := &tradepb.DeleteTradeRequest{
		TradeId:     tradeID,
		PortfolioId: portfolioID,
	}

	_, err := client.DeleteTrade(context.Background(), req)
	if err != nil {
		logger.Fatalf("Error calling DeleteTrade: %v", err)
		return err
	}

	return nil
}

// GetTrade retrieves the trade from the databsse with a given _id
func GetTrade(tradeID string, portfolioID int32, client tradepb.TradeServiceClient) (*tradepb.Trade, error) {
	req := &tradepb.GetTradeRequest{
		TradeId:     tradeID,
		PortfolioId: portfolioID,
	}

	res, err := client.GetTrade(context.Background(), req)
	if err != nil {
		logger.Printf("Error calling GetTrade: %v", err)
		return nil, err
	}
	return res.GetTrade(), nil
}

// UpdateTrade updates a trade with a given _id to a specified trade
func UpdateTrade(tradeID string, trade *tradepb.Trade, portfolioID int32, client tradepb.TradeServiceClient) error {
	req := &tradepb.UpdateTradeRequest{
		TradeId:     tradeID,
		PortfolioId: portfolioID,
		Trade:       trade,
	}

	_, err := client.UpdateTrade(context.Background(), req)
	if err != nil {
		logger.Printf("Error calling UpdateTrade with tradeID %v: %v", tradeID, err)
		return err
	}
	return nil
}

// GetAllTrades returns a stream of all trades in a portfolio
func GetAllTrades(portfolioID int32, client tradepb.TradeServiceClient) error {
	req := &tradepb.GetAllTradesRequest{
		PortfolioId: portfolioID,
	}

	stream, err := client.GetAllTrades(context.Background(), req)
	if err != nil {
		logger.Printf("Error calling GetAllTrades with portfolio %v: %v", portfolioID, err)
		return err
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Printf("Error receiving trade on GetAllTrades stream: %v", err)
		}

		log.Println(msg.GetTrade())
	}
}

// rowify parses a trade into a string that represents a csv row
func rowify(trade *tradepb.Trade) string {
	val := reflect.Indirect(reflect.ValueOf(trade))
	row := ""

	for i := 0; i < val.NumField()-3; i++ {
		elem := val.Field(i)
		if i == 0 {
			row = fmt.Sprintf("%v", elem)
		} else {
			row = fmt.Sprintf("%s,%v", row, elem)
		}
	}
	return row
}

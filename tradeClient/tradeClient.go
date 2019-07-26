package main

import (
	"context"
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
	f, err := os.OpenFile("tradeClient/log.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("Unable to open log file")
	}
	defer f.Close()

	logger = log.New(f, time.Now().Format("01-02-2006 15:04:05 "), 0)
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
		logger.Println("Error calling UpdateTrade with tradeID %v: %v", tradeID, err)
		return err
	}
	return nil
}

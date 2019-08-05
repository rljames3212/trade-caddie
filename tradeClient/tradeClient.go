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
	"strconv"
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
func GetAllTrades(portfolioID int32, client tradepb.TradeServiceClient) ([]*tradepb.Trade, error) {
	req := &tradepb.GetAllTradesRequest{
		PortfolioId: portfolioID,
	}

	stream, err := client.GetAllTrades(context.Background(), req)
	if err != nil {
		logger.Printf("Error calling GetAllTrades with portfolio %v: %v", portfolioID, err)
		return nil, err
	}

	trades := []*tradepb.Trade{}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return trades, nil
		}
		if err != nil {
			logger.Printf("Error receiving trade on GetAllTrades stream: %v", err)
		}
		trades = append(trades, msg.GetTrade())
	}
}

// Export writes a slice of trades to a csv file named export.csv
func Export(trades []*tradepb.Trade, client tradepb.TradeServiceClient) error {
	stream, err := client.Export(context.Background())
	if err != nil {
		logger.Printf("Error creating export stream: %v", err)
		return err
	}

	for _, trade := range trades {
		err = stream.Send(&tradepb.ExportRequest{
			Trade: trade,
		})
		if err != nil {
			logger.Printf("Error sending trade on export stream: %v", err)
			return err
		}
	}
	result, err := stream.CloseAndRecv()
	if err != nil {
		logger.Printf("Error receiving response from server on export: %v", err)
		return err
	}

	logger.Printf("%v trades exported to csv", result.NumTrades)
	return nil
}

// importTrades receives a slice of trades and imports them to the specified portfolio
func importTrades(trades []*tradepb.Trade, portfolioID int32, client tradepb.TradeServiceClient) error {
	stream, err := client.Import(context.Background())
	if err != nil {
		logger.Printf("Error creating import stream: %v", err)
		return err
	}

	for _, trade := range trades {
		err = stream.Send(&tradepb.ImportRequest{
			Trade:       trade,
			PortfolioId: portfolioID,
		})
		if err != nil {
			logger.Printf("Error sending trade on import stream: %v", err)
			return err
		}
	}
	result, err := stream.CloseAndRecv()
	if err != nil {
		logger.Printf("Error receiving response from server on export: %v", err)
		return err
	}

	logger.Printf("%v trades imported", result.NumImported)
	return nil
}

// GetTradesByMarket gets all trades in a specific market from a portfolio
func GetTradesByMarket(market string, portfolioID int32, client tradepb.TradeServiceClient) ([]*tradepb.Trade, error) {
	req := &tradepb.GetTradesByMarketRequest{
		Market:      market,
		PortfolioId: portfolioID,
	}

	stream, err := client.GetTradesByMarket(context.Background(), req)
	if err != nil {
		logger.Printf("Error calling GetTradesByMarket with market: %v and portfolio %v: %v", market, portfolioID, err)
		return nil, err
	}

	trades := []*tradepb.Trade{}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			return trades, nil
		}
		if err != nil {
			logger.Printf("Error receiving trade on GetTradesByMarket stream: %v", err)
			return nil, err
		}
		trades = append(trades, res.GetTrade())
	}

}

// GetTradesByDateRange returns a slice of trades that are within a provided date range from a portfolio
func GetTradesByDateRange(startDate, endDate string, portfolioID int32, client tradepb.TradeServiceClient) ([]*tradepb.Trade, error) {
	req := &tradepb.GetTradesByDateRangeRequest{
		StartDate:   startDate,
		EndDate:     endDate,
		PortfolioId: portfolioID,
	}

	stream, err := client.GetTradesByDateRange(context.Background(), req)
	if err != nil {
		logger.Printf("Error calling GetTradesByDateRange with start date: %v , end date: %v , and portfolio %v: %v", startDate, endDate, portfolioID, err)
		return nil, err
	}

	trades := []*tradepb.Trade{}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			return trades, nil
		}
		if err != nil {
			logger.Printf("Error receiving trade on GetTradesByMarket stream: %v", err)
			return nil, err
		}
		trades = append(trades, res.GetTrade())
	}
}

// parseRow parses a csv row into a trade
func parseRow(row []string) (*tradepb.Trade, error) {
	trade := tradepb.Trade{}
	val := reflect.ValueOf(&trade)
	for i, field := range row {
		tradeField := val.Elem().Field(i)
		switch tradeField.Interface().(type) {
		case string:
			temp := string([]byte(field))
			tradeField.SetString(temp)
		case int32, int64:
			num, err := strconv.Atoi(field)
			if err != nil {
				logger.Printf("Error parsing string ( %v ) to int: %v", field, err)
				return nil, err
			}
			tradeField.SetInt(int64(num))
		case float32:
			num, err := strconv.ParseFloat(field, 32)
			if err != nil {
				logger.Printf("Error parsing string ( %v ) to float: %v", field, err)
				return nil, err
			}
			tradeField.SetFloat(num)
		case bool:
			b, err := strconv.ParseBool(field)
			if err != nil {
				logger.Printf("Error parsing string ( %v ) to bool: %v", field, err)
				return nil, err
			}
			tradeField.SetBool(b)
		case tradepb.Trade_Type:
			x := tradepb.Trade_Type_value[field]
			tradeField.Set(reflect.ValueOf(tradepb.Trade_Type(x)))
		default:
			err := fmt.Errorf("Error unexpected type parsing string ( %v )", field)
			logger.Println(err)
			return nil, err
		}

	}
	return &trade, nil
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
	"trade-caddie/tradepb"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
)

var port *string
var logger *log.Logger
var loggerFile *os.File
var db *mongo.Client

type server struct{}

func init() {
	// initialize command line flags
	port = flag.String("port", ":5000", "port to run server on")
	flag.Parse()

	// initialize logger
	loggerFile, err := os.OpenFile("tradeServer/log.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("Error opening log file")
	}

	logger = log.New(loggerFile, time.Now().Format("01-02-2006 15:04:05 "), 0)

	// create database client
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	db, err = mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		logger.Fatalf("Error creating database client: %v", err)
	}
}

func main() {
	log.Printf("Starting server on port %v\n", *port)
	lis, err := net.Listen("tcp", *port)
	if err != nil {
		logger.Fatalf("Failed to listen on port %v - %v\n", *port, err)
	}

	server := &server{}
	grpcServer := grpc.NewServer()
	tradepb.RegisterTradeServiceServer(grpcServer, server)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGTERM)
	signal.Notify(stopChan, syscall.SIGINT)
	
	go func() {
		sig := <-stopChan
		logger.Printf("signal: %+v received. Shutting down", sig)
		defer loggerFile.Close()
		db.Disconnect(context.Background())
		os.Exit(0)
	}()

	// start server
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("Failed to serve: %v", err)
	}
}

// AddTrade adds a trade to a portfolio
func (*server) AddTrade(ctx context.Context, req *tradepb.AddTradeRequest) (*tradepb.AddTradeResponse, error) {
	portfolioID := req.GetPortfolioId()
	trade := req.GetTrade()

	trade.XId = primitive.NewObjectID().Hex()

	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	insertResult, err := tradeCollection.InsertOne(ctx, trade)

	if err != nil {
		logger.Printf("Error inserting trade to portfolio %v: %v", portfolioID, err)
		return nil, err
	}

	res := &tradepb.AddTradeResponse{
		TradeId: insertResult.InsertedID.(string),
	}
	return res, nil
}

// DeleteTrade deletes a trade from a portfolio
func (*server) DeleteTrade(ctx context.Context, req *tradepb.DeleteTradeRequest) (*tradepb.DeleteTradeResponse, error) {
	portfolioID := req.GetPortfolioId()
	tradeID := req.GetTradeId()

	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	filter := bson.D{{Key: "_id", Value: tradeID}}
	deleteResult, err := tradeCollection.DeleteOne(ctx, filter)

	if err != nil {
		logger.Printf("Error deleting trade with ID %v from portfolio %v: %v", tradeID, portfolioID, err)
		return nil, err
	}

	res := &tradepb.DeleteTradeResponse{
		DeletedCount: int32(deleteResult.DeletedCount),
	}

	return res, nil
}

// UpdateTrade updates a trade
func (*server) UpdateTrade(ctx context.Context, req *tradepb.UpdateTradeRequest) (*tradepb.UpdateTradeResponse, error) {
	portfolioID := req.GetPortfolioId()
	updatedTrade := req.GetTrade()
	tradeID := req.GetTradeId()

	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	filter := bson.D{{Key: "_id", Value: tradeID}}
	update := bson.M{"$set": updatedTrade}
	updateResult, err := tradeCollection.UpdateOne(ctx, filter, update)

	if err != nil {
		logger.Printf("Error updating trade with _id %v in portfolio %v: %v", tradeID, portfolioID, err)
		return nil, err
	}

	res := &tradepb.UpdateTradeResponse{
		ModifiedCount: int32(updateResult.ModifiedCount),
	}

	return res, nil
}

// GetTrade retrieves a trade from a portfolio
func (*server) GetTrade(ctx context.Context, req *tradepb.GetTradeRequest) (*tradepb.GetTradeResponse, error) {
	portfolioID := req.GetPortfolioId()
	tradeID := req.GetTradeId()

	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	filter := bson.D{{Key: "_id", Value: tradeID}}

	var trade tradepb.Trade
	err := tradeCollection.FindOne(ctx, filter).Decode(&trade)

	if err != nil {
		logger.Printf("Error retrieving trade with _id %v from portfolio %v: %v", tradeID, portfolioID, err)
		return nil, err
	}

	res := &tradepb.GetTradeResponse{
		Trade: &trade,
	}

	return res, nil
}

package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"
	"trade-caddie/tradepb"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	logger = log.New(loggerFile, time.Now().Format("01-02-2006T15:04:05 "), 0)

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

	// channel to receive interrupt command
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGTERM)
	signal.Notify(stopChan, syscall.SIGINT)

	// cleanup resources on interrupt
	go func() {
		sig := <-stopChan
		logger.Printf("signal: %+v received. Shutting down", sig)
		defer loggerFile.Close()
		db.Disconnect(context.Background())
		grpcServer.Stop()
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
	if trade.GetDate() == 0 {
		trade.Date = time.Now().Unix()
	}

	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "Client canceled AddTrade request")
	}

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

	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "Client canceled DeleteTrade request")
	}

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

// DeleteTrades deletes trades from a portfolio whose ID is in a slice of IDs
func (*server) DeleteTrades(ctx context.Context, req *tradepb.DeleteTradesRequest) (*tradepb.DeleteTradesResponse, error) {
	ids := req.GetId()
	portfolioID := req.GetPortfolioId()

	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	filter := bson.M{
		"_id": bson.M{
			"$in": ids,
		},
	}

	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "Client canceled DeleteTrades request")
	}

	res, err := tradeCollection.DeleteMany(ctx, filter)
	if err != nil {
		logger.Printf("Error deleting trades from database: %v", err)
		return nil, err
	}

	logger.Printf("Deleted %v trades from portfolio %v", res.DeletedCount, portfolioID)
	return &tradepb.DeleteTradesResponse{
		DeletedCount: int32(res.DeletedCount),
	}, nil
}

// DeleteAllTrades deletes all trades in a portfolio
func (*server) DeleteAllTrades(ctx context.Context, req *tradepb.DeleteAllTradesRequest) (*tradepb.DeleteAllTradesResponse, error) {
	portfolioID := req.GetPortfolioId()

	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	filter := bson.D{{}}

	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "Client canceled DeleteAllTrades request")
	}

	res, err := tradeCollection.DeleteMany(ctx, filter)
	if err != nil {
		logger.Printf("Error deleting all trades in portfolio %v: %v", portfolioID, err)
		return nil, err
	}

	return &tradepb.DeleteAllTradesResponse{
		DeletedCount: int32(res.DeletedCount),
	}, nil
}

// UpdateTrade updates a trade
func (*server) UpdateTrade(ctx context.Context, req *tradepb.UpdateTradeRequest) (*tradepb.UpdateTradeResponse, error) {
	portfolioID := req.GetPortfolioId()
	updatedTrade := req.GetTrade()
	tradeID := req.GetTradeId()

	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	filter := bson.D{{Key: "_id", Value: tradeID}}
	update := bson.M{"$set": updatedTrade}

	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "Client canceled UpdateTrade request")
	}

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

	if ctx.Err() == context.Canceled {
		return nil, status.Error(codes.Canceled, "Client canceled GetTrade request")
	}

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

// GetAllTrades returns a stream of all trades in a portfolio
func (*server) GetAllTrades(req *tradepb.GetAllTradesRequest, stream tradepb.TradeService_GetAllTradesServer) error {
	portfolioID := req.GetPortfolioId()
	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	filter := bson.D{}

	// get all trades
	cursor, err := tradeCollection.Find(context.TODO(), filter)
	if err != nil {
		logger.Printf("Error retrieving all trades from portfolio %v: %v", portfolioID, err)
		return err
	}
	defer cursor.Close(context.Background())

	// iterate through trades and send each on the stream
	for cursor.Next(context.Background()) {
		var result tradepb.Trade
		err = cursor.Decode(&result)

		if err != nil {
			logger.Printf("Error decoding trade in portfolio %v: %v", portfolioID, err)
		}

		sendErr := stream.Send(&tradepb.GetAllTradesResponse{
			Trade: &result,
		})
		if sendErr != nil {
			logger.Printf("Error returning trade: %v", err)
		}
	}
	return nil
}

// Export receives a stream of trades and exports them as a csv file
func (*server) Export(stream tradepb.TradeService_ExportServer) error {
	csvfile, err := os.OpenFile("export.csv", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		logger.Printf("Error opening export file: %v", err)
		return err
	}
	defer csvfile.Close()

	csvwriter := csv.NewWriter(csvfile)
	if err := csvwriter.Write(getTradeHeaders()); err != nil {
		logger.Printf("Error writing headers to csv: %v", err)
		return err
	}
	tradeCount := int32(0)
	for {
		trade, err := stream.Recv()
		if err == io.EOF {
			csvwriter.Flush()
			if err = csvwriter.Error(); err != nil {
				logger.Printf("Error flushing csv writer: %v", err)
				return err
			}
			return stream.SendAndClose(&tradepb.ExportResponse{
				NumTrades: tradeCount,
			})
		}
		tradeCount++
		row := rowify(trade.GetTrade())

		if err := csvwriter.Write(row); err != nil {
			logger.Printf("Error writing trade to csv( %v ): %v", row, err)
			return err
		}
	}
}

// Import receives a stream of trades and imports each into a specified portfolio
// note: all ImportRequests must have the same portfolioID
func (*server) Import(stream tradepb.TradeService_ImportServer) error {
	trades := []interface{}{}
	var portfolioID int32
	numTrades := int32(0)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
			result, err := tradeCollection.InsertMany(context.Background(), trades)
			if err != nil {
				logger.Printf("Error importing trades to portfolio %v: %v", portfolioID, err)
				return err
			}

			logger.Printf("Imported %v trades to portfolio %v", len(result.InsertedIDs), portfolioID)
			return stream.SendAndClose(&tradepb.ImportResponse{
				NumImported: numTrades,
			})
		}
		if err != nil {
			logger.Printf("Error receiving on import stream: %v", err)
			return err
		}

		trade := req.GetTrade()
		trade.XId = primitive.NewObjectID().Hex()
		if trade.GetDate() == 0 {
			trade.Date = time.Now().Unix()
		}

		trades = append(trades, trade)
		portfolioID = req.GetPortfolioId()
		numTrades++
	}
}

// GetTradesByMarket returns a stream of trade objects that belong to a specified market
func (*server) GetTradesByMarket(req *tradepb.GetTradesByMarketRequest, stream tradepb.TradeService_GetTradesByMarketServer) error {
	market := req.GetMarket()
	portfolioID := req.GetPortfolioId()

	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	filter := bson.D{{Key: "market", Value: market}}

	cursor, err := tradeCollection.Find(context.TODO(), filter)
	if err != nil {
		logger.Printf("Error querying database in GetTradesByMarket ( %v ): %v", market, err)
		return err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var result tradepb.Trade
		err = cursor.Decode(&result)

		if err != nil {
			logger.Printf("Error decoding trade in portfolio %v: %v", portfolioID, err)
		}

		sendErr := stream.Send(&tradepb.GetTradesByMarketResponse{
			Trade: &result,
		})
		if sendErr != nil {
			logger.Printf("Error returning trade: %v", err)
		}
	}
	return nil
}

// GetTradesByDateRange returns a stream of trades within a specified date range
func (*server) GetTradesByDateRange(req *tradepb.GetTradesByDateRangeRequest, stream tradepb.TradeService_GetTradesByDateRangeServer) error {
	startDate, err := time.Parse("2006-01-02 15:04:05", req.GetStartDate())
	if err != nil {
		logger.Printf("Error parsing startDate ( %v ) to timestamp: %v", startDate, err)
		return err
	}
	endDate, err := time.Parse("2006-01-02 15:04:05", req.GetEndDate())
	if err != nil {
		logger.Printf("Error parsing endDate ( %v ) to timestamp: %v", endDate, err)
		return err
	}
	portfolioID := req.GetPortfolioId()

	tradeCollection := db.Database("trade-caddie").Collection(fmt.Sprintf("portfolio_%v", portfolioID))
	filter := bson.M{
		"$and": bson.A{
			bson.M{"date": bson.M{"$gte": startDate.Unix()}},
			bson.M{"date": bson.M{"$lt": endDate.Unix()}},
		},
	}

	cursor, err := tradeCollection.Find(context.Background(), filter)
	if err != nil {
		logger.Printf("Error querying database in GetTradesByDateRange ( %v , %v ): %v", startDate, endDate, err)
		return err
	}
	defer cursor.Close(context.Background())

	var result tradepb.Trade
	for cursor.Next(context.Background()) {
		err = cursor.Decode(&result)
		if err != nil {
			logger.Printf("Error decoding trade in portfolio %v: %v", portfolioID, err)
		}

		sendErr := stream.Send(&tradepb.GetTradesByDateRangeResponse{
			Trade: &result,
		})
		if sendErr != nil {
			logger.Printf("Error returning trade: %v", err)
		}
	}
	return nil
}

// rowify parses a trade into a string that represents a csv row
func rowify(trade *tradepb.Trade) []string {
	val := reflect.Indirect(reflect.ValueOf(trade))
	row := []string{}

	// skip _id and fields generated by protoc
	for i := 1; i < val.NumField()-3; i++ {
		elem := val.Field(i)
		row = append(row, fmt.Sprintf("%v", elem))
	}
	return row
}

// getTradeHeaders returns a slice oh headers to be written to a csv file based on trade field names
func getTradeHeaders() []string {
	trade := &tradepb.Trade{}
	t := reflect.TypeOf(trade)
	headers := []string{}
	tagName := "csv"

	for i := 0; i < t.Elem().NumField(); i++ {
		csvtag := t.Elem().Field(i).Tag.Get(tagName)
		if csvtag != "" {
			headers = append(headers, csvtag)
		}
	}
	return headers
}

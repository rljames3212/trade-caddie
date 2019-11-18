package tradeclient

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"trade-caddie/tradepb"

	"google.golang.org/grpc"
)

var logger *log.Logger
var logFile *os.File

// TradeClient is a wrapper for tradepb.TradeServiceClient
type TradeClient struct {
	client  tradepb.TradeServiceClient
	logger  *log.Logger
	logFile *os.File
}

// NewTradeClient returns a tradeClient connected to the TradeServer running on port
func NewTradeClient(conn *grpc.ClientConn) TradeClient {
	// initialize client
	client := tradepb.NewTradeServiceClient(conn)

	// initialize logger
	logFile, err := os.OpenFile("tradeClient/log.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Unable to open log file")
	}

	logger = log.New(logFile, time.Now().Format("01-02-2006 15:04:05 "), 0)
	logger.Printf("Starting new client")

	return TradeClient{
		client:  client,
		logger:  logger,
		logFile: logFile,
	}
}

// Disconnect cleans up the log file
func (tc *TradeClient) Disconnect() {
	tc.logFile.Close()
}

// AddTrade adds a trade to a portfolio
func (tc *TradeClient) AddTrade(trade *tradepb.Trade, portfolioID int32) (string, error) {
	req := &tradepb.AddTradeRequest{
		Trade:       trade,
		PortfolioId: portfolioID,
	}
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	res, err := tc.client.AddTrade(ctx, req)
	if err != nil {
		logger.Printf("Error calling AddTrade: %v", err)
		return "", err
	}

	return res.GetTradeId(), nil
}

// DeleteTrade deletes a trade from the database given an _id
func (tc *TradeClient) DeleteTrade(tradeID string, portfolioID int32) error {
	req := &tradepb.DeleteTradeRequest{
		TradeId:     tradeID,
		PortfolioId: portfolioID,
	}
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	_, err := tc.client.DeleteTrade(ctx, req)
	if err != nil {
		logger.Fatalf("Error calling DeleteTrade: %v", err)
		return err
	}

	return nil
}

// DeleteTrades deletes all trades with given IDs
func (tc *TradeClient) DeleteTrades(ids []string, portfolioID int32) error {
	req := &tradepb.DeleteTradesRequest{
		Id:          ids,
		PortfolioId: portfolioID,
	}
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	res, err := tc.client.DeleteTrades(ctx, req)
	if err != nil {
		logger.Printf("Error calling DeleteTrades: %v", err)
		return err
	}

	logger.Printf("Deleted %v trades from portfolio %v", res.GetDeletedCount(), portfolioID)
	return nil
}

// DeleteAllTrades deletes all trades in a portfolio
func (tc *TradeClient) DeleteAllTrades(portfolioID int32) error {
	req := &tradepb.DeleteAllTradesRequest{
		PortfolioId: portfolioID,
	}
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	res, err := tc.client.DeleteAllTrades(ctx, req)
	if err != nil {
		logger.Printf("Error calling DeleteAllTrades: %v", err)
		return err
	}
	logger.Printf("Deleted %v trades from portfolio: %v", res.GetDeletedCount(), portfolioID)
	return nil
}

// GetTrade retrieves the trade from the databsse with a given _id
func (tc *TradeClient) GetTrade(tradeID string, portfolioID int32) (*tradepb.Trade, error) {
	req := &tradepb.GetTradeRequest{
		TradeId:     tradeID,
		PortfolioId: portfolioID,
	}
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	res, err := tc.client.GetTrade(ctx, req)
	if err != nil {
		logger.Printf("Error calling GetTrade: %v", err)
		return nil, err
	}
	return res.GetTrade(), nil
}

// UpdateTrade updates a trade with a given _id to a specified trade
func (tc *TradeClient) UpdateTrade(tradeID string, trade *tradepb.Trade, portfolioID int32) error {
	req := &tradepb.UpdateTradeRequest{
		TradeId:     tradeID,
		PortfolioId: portfolioID,
		Trade:       trade,
	}
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	_, err := tc.client.UpdateTrade(ctx, req)
	if err != nil {
		logger.Printf("Error calling UpdateTrade with tradeID %v: %v", tradeID, err)
		return err
	}
	return nil
}

// GetAllTrades returns a stream of all trades in a portfolio
func (tc *TradeClient) GetAllTrades(portfolioID int32) ([]*tradepb.Trade, error) {
	req := &tradepb.GetAllTradesRequest{
		PortfolioId: portfolioID,
	}
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	stream, err := tc.client.GetAllTrades(ctx, req)
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
func (tc *TradeClient) Export(trades []*tradepb.Trade) error {
	stream, err := tc.client.Export(context.Background())
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

// GetTradesByMarket gets all trades in a specific market from a portfolio
func (tc *TradeClient) GetTradesByMarket(market string, portfolioID int32) ([]*tradepb.Trade, error) {
	req := &tradepb.GetTradesByMarketRequest{
		Market:      market,
		PortfolioId: portfolioID,
	}
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	stream, err := tc.client.GetTradesByMarket(ctx, req)
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
func (tc *TradeClient) GetTradesByDateRange(startDate, endDate string, portfolioID int32) ([]*tradepb.Trade, error) {
	req := &tradepb.GetTradesByDateRangeRequest{
		StartDate:   startDate,
		EndDate:     endDate,
		PortfolioId: portfolioID,
	}
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	stream, err := tc.client.GetTradesByDateRange(ctx, req)
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

// ImportFromCSV imports trades into a portfolio from a csv file
func (tc *TradeClient) ImportFromCSV(filename string, portfolioID int32) error {
	csvfile, err := os.Open(filename)
	if err != nil {
		logger.Printf("Error opening %v for import: %v", filename, err)
		return err
	}
	defer csvfile.Close()

	csvreader := csv.NewReader(csvfile)
	headers, err := csvreader.Read()
	if err != nil {
		logger.Printf("Error reading headers in %v: %v", filename, err)
		return err
	}

	var updatedHeaders []string
	for _, header := range headers {
		updatedHeaders = append(updatedHeaders, strings.ToUpper(header))
	}

	rows, err := csvreader.ReadAll()
	if err != nil {
		logger.Printf("Error reading csv rows in %v: %v", filename, err)
		return err
	}

	trades := []*tradepb.Trade{}
	for _, row := range rows {
		trade, err := parseRow(updatedHeaders, row)
		if err != nil {
			logger.Printf("Error parsing row to trade: %v", err)
		}
		trades = append(trades, trade)
	}

	return importTrades(trades, portfolioID, tc.client)
}

// GetBalance returns the balance of a certain coin in a given portfolio
func GetBalance(coinID string, portfolioID int32, client tradepb.TradeServiceClient) (float32, error) {
	req := &tradepb.GetBalanceRequest{
		Coin:        coinID,
		PortfolioId: portfolioID,
	}

	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	res, err := client.GetBalance(ctx, req)
	if err != nil {
		logger.Printf("Error calling GetBalance in portfolio %v with coin %v: %v", portfolioID, coinID, err)
		return 0.0, err
	}

	return res.GetBalance(), nil
}

// importTrades receives a slice of trades and imports them to the specified portfolio
func importTrades(trades []*tradepb.Trade, portfolioID int32, client tradepb.TradeServiceClient) error {
	clientDeadline := time.Now().Add(time.Duration(1 * time.Second))
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	stream, err := client.Import(ctx)
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

// parseRow parses a csv row to a trade. Returns an error if row can't be parsed
func parseRow(headers, row []string) (*tradepb.Trade, error) {
	trade := tradepb.Trade{}
	val := reflect.ValueOf(&trade)

	feeIndex := 0
	for i, header := range headers {
		tradeFieldIndex, err := matchHeaderToTradeField(header)
		if err != nil {
			continue
		}
		tradeField := val.Elem().Field(tradeFieldIndex)

		if strings.Contains(header, "DATE") {
			ts, err := parseDate(row[i])
			if err != nil {
				log.Printf("Error unable to parse date: %v", err)
			}
			tradeField.SetInt(ts)
		} else if header == "FEE" {
			feeIndex = i
			continue
		} else {
			switch tradeField.Interface().(type) {
			case string:
				temp := string([]byte(row[i]))
				tradeField.SetString(temp)
			case int32, int64:
				num, err := strconv.Atoi(row[i])
				if err != nil {
					log.Printf("Error parsing string ( %v ) to int: %v", row[i], err)
					return nil, err
				}
				tradeField.SetInt(int64(num))
			case float32:
				num, err := strconv.ParseFloat(row[i], 32)
				if err != nil {
					log.Printf("Error parsing string ( %v ) to float: %v", row[i], err)
					return nil, err
				}
				tradeField.SetFloat(num)
			case bool:
				b, err := strconv.ParseBool(row[i])
				if err != nil {
					log.Printf("Error parsing string ( %v ) to bool: %v", row[i], err)
					return nil, err
				}
				tradeField.SetBool(b)
			case tradepb.Trade_Type:
				x := tradepb.Trade_Type_value[strings.ToUpper(row[i])]
				tradeField.Set(reflect.ValueOf(tradepb.Trade_Type(x)))
			default:
				err := fmt.Errorf("Error unexpected type parsing string ( %v )", row[i])
				log.Println(err)
				return nil, err
			}
		}
	}

	// set fee field after all other fields have been set
	f, err := parseFee(row[feeIndex], &val)
	if err != nil {
		log.Printf("Error unable to parse fee: %v", err)
		return nil, err
	}
	val.Elem().FieldByName("Fee").SetFloat(float64(f))

	return &trade, nil
}

// matchHeaderToTradeField returns the index of a field in a tradepb.Trade type that has a provided csv struct tag
func matchHeaderToTradeField(fieldName string) (int, error) {
	val := reflect.TypeOf(tradepb.Trade{})
	for i := 0; i < val.NumField(); i++ {
		tag := val.Field(i).Tag.Get("csv")
		if tag == fieldName {
			return i, nil
		}
		if strings.Contains(fieldName, "DATE") && tag == "DATE" {
			return i, nil
		}
	}
	return 0, fmt.Errorf("Header not found")
}

func parseDate(date string) (int64, error) {
	// check if date is a timestamp
	timestamp, err := strconv.Atoi(date)
	if err == nil {
		return int64(timestamp), nil
	}

	parsedTime, err := time.Parse("2006-01-02 15:04:05", date)
	if err != nil {
		return 0, err
	}
	return parsedTime.Unix(), nil
}

func parseFee(fee string, trade *reflect.Value) (float32, error) {
	var parsedFee float64
	if strings.HasSuffix(fee, "%") {
		percent, err := strconv.ParseFloat(strings.TrimSuffix(fee, "%"), 32)
		if err != nil {
			logger.Printf("Error parsing fee percentage ( %v ): %v", fee, err)
			return float32(0), err
		}

		tradePriceIndex, _ := matchHeaderToTradeField("PRICE")
		tradeAmountIndex, _ := matchHeaderToTradeField("AMOUNT")

		p := trade.Elem().Field(tradePriceIndex).Float()
		a := trade.Elem().Field(tradeAmountIndex).Float()
		parsedFee = p * a * percent
		return float32(parsedFee), nil
	}
	parsedFee, err := strconv.ParseFloat(fee, 32)
	if err != nil {
		logger.Printf("Error parsing fee to float ( %v ): %v", fee, err)
		return float32(0), err
	}
	return float32(parsedFee), nil
}

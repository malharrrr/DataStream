package sources

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	pb "github.com/malharrrr/datastream/proto"
)

type binanceTrade struct {
	EventType string `json:"e"` 
	EventTime int64  `json:"E"` 
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
}

type BinanceSource struct {
	symbol string 
}

func NewBinanceSource(symbol string) *BinanceSource {
	return &BinanceSource{symbol: strings.ToLower(symbol)}
}

func (b *BinanceSource) Name() string {
	return "BINANCE_WS_" + strings.ToUpper(b.symbol)
}

func (b *BinanceSource) Start(ctx context.Context, out chan<- FetchResult) {
	url := "wss://stream.binance.com:9443/ws/" + b.symbol + "@trade"

	slog.Info("Dialing Binance WebSocket...", "url", url)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		out <- FetchResult{Err: err}
		return
	}
	defer conn.Close()

	slog.Info("Successfully connected to Binance!", "symbol", b.symbol)

	errChan := make(chan error)

	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				errChan <- err
				return
			}
			var trade binanceTrade
			if err := json.Unmarshal(message, &trade); err != nil {
				slog.Warn("Failed to parse Binance JSON", "err", err, "raw", string(message))
				continue
			}

			price, _ := strconv.ParseFloat(trade.Price, 64)
			quantityFloat, _ := strconv.ParseFloat(trade.Quantity, 64)
			volume := int64(quantityFloat * 100000000) 

			tick := &pb.Tick{
				Symbol:       strings.ToUpper(b.symbol),
				Exchange:     "BINANCE",
				TimestampMs:  trade.EventTime,
				Open:         price, 
				High:         price,
				Low:          price,
				Close:        price,
				Volume:       volume,
				Vwap:         price,
				TickCount:    1,
				DataSource:   "binance_ws",
				ReceivedAtMs: time.Now().UnixMilli(),
			}
			out <- FetchResult{
				Ticks: []*pb.Tick{tick},
				Err:   nil,
			}
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("Closing Binance WebSocket gracefully", "symbol", b.symbol)
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		return
	case err := <-errChan:
		out <- FetchResult{Err: err}
		return
	}
}
package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	pb "github.com/malharrrr/datastream/proto"
)

type yahooResponse struct {
	Chart struct {
		Result []struct {
			Meta struct {
				Symbol              string  `json:"symbol"`
				RegularMarketPrice  float64 `json:"regularMarketPrice"`
				RegularMarketTime   int64   `json:"regularMarketTime"`
				RegularMarketVolume int64   `json:"regularMarketVolume"`
			} `json:"meta"`
		} `json:"result"`
	} `json:"chart"`
}

type YahooSource struct {
	symbol string  
	client *http.Client
}

func NewYahooSource(symbol string) *YahooSource {
	return &YahooSource{
		symbol: symbol,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (y *YahooSource) Name() string {
	return "YAHOO_REST_" + strings.ToUpper(y.symbol)
}

func (y *YahooSource) Start(ctx context.Context, out chan<- FetchResult) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	url := fmt.Sprintf("https://query1.finance.yahoo.com/v8/finance/chart/%s", y.symbol)
	var lastTimestamp int64

	for {
		select {
		case <-ctx.Done():
			slog.Info("Closing Yahoo REST source gracefully", "symbol", y.symbol)
			return
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				out <- FetchResult{Err: err}
				continue
			}
			req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

			resp, err := y.client.Do(req)
			if err != nil {
				out <- FetchResult{Err: err}
				continue
			}

			if resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				out <- FetchResult{Err: fmt.Errorf("bad status code: %d", resp.StatusCode)}
				continue
			}

			var data yahooResponse
			if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
				resp.Body.Close()
				out <- FetchResult{Err: err}
				continue
			}
			resp.Body.Close()

			if len(data.Chart.Result) == 0 {
				continue
			}

			meta := data.Chart.Result[0].Meta

			if meta.RegularMarketTime == lastTimestamp {
				continue // Skip the rest of the loop and wait for the next tick
			}
			lastTimestamp = meta.RegularMarketTime

			tick := &pb.Tick{
				Symbol:       strings.ToUpper(y.symbol),
				Exchange:     "NSE",
				TimestampMs:  meta.RegularMarketTime * 1000,
				Open:         meta.RegularMarketPrice,
				High:         meta.RegularMarketPrice,
				Low:          meta.RegularMarketPrice,
				Close:        meta.RegularMarketPrice,
				Volume:       meta.RegularMarketVolume,
				Vwap:         meta.RegularMarketPrice,
				TickCount:    1,
				DataSource:   "yahoo_rest",
				ReceivedAtMs: time.Now().UnixMilli(),
			}

			out <- FetchResult{
				Ticks: []*pb.Tick{tick},
				Err:   nil,
			}
		}
	}
}
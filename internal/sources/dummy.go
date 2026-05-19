package sources

import (
	"context"
	"math/rand"
	"time"

	pb "github.com/malharrrr/datastream/proto"
)

type DummySource struct {
	symbol string
}

func NewDummySource(symbol string) *DummySource {
	return &DummySource{symbol: symbol}
}

func (d *DummySource) Name() string {
	return "DUMMY_GENERATOR_" + d.symbol
}

func (d *DummySource) Start(ctx context.Context, out chan<- FetchResult) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	basePrice := 100.0

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			change := (rand.Float64() - 0.5) * 2 
			basePrice += change

			tick := &pb.Tick{
				Symbol:       d.symbol,
				Exchange:     "SIMULATION",
				TimestampMs:  t.UnixMilli(),
				Open:         basePrice - 0.1,
				High:         basePrice + 0.5,
				Low:          basePrice - 0.5,
				Close:        basePrice,
				Volume:       int64(rand.Intn(1000)),
				Vwap:         basePrice,
				TickCount:    1,
				DataSource:   "dummy_routine",
				ReceivedAtMs: time.Now().UnixMilli(),
			}

			out <- FetchResult{
				Ticks: []*pb.Tick{tick},
				Err:   nil,
			}
		}
	}
}
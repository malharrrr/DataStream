package ingestion

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/malharrrr/datastream/internal/sources"
	"github.com/malharrrr/datastream/internal/storage"
	pb "github.com/malharrrr/datastream/proto"
	"golang.org/x/time/rate"
)

type Pipeline struct {
	sources     []sources.Source
	repo        *storage.TimescaleRepo
	cache       *storage.TickCache
	tickChan    chan *pb.Tick 
	concurrency int
}

func NewPipeline(repo *storage.TimescaleRepo, cache *storage.TickCache, bufferSize int) *Pipeline {
	return &Pipeline{
		sources: make([]sources.Source, 0),
		repo:    repo,
		cache:  cache,
		tickChan:    make(chan *pb.Tick, bufferSize),
		concurrency: 5, 
	}
}

func (p *Pipeline) AddSource(s sources.Source) {
	p.sources = append(p.sources, s)
}

func (p *Pipeline) Start(ctx context.Context) {
	var wg sync.WaitGroup
	slog.Info("Starting DB writer pool", "workers", p.concurrency)
	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)
		go p.dbWriter(ctx, &wg)
	}

	for _, src := range p.sources {
		wg.Add(1)
		go p.runSource(ctx, src, &wg)
	}

	go func() {
		wg.Wait()
		close(p.tickChan)
		slog.Info("Pipeline stopped gracefully")
	}()
}

func (p *Pipeline) runSource(ctx context.Context, src sources.Source, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("Starting source worker", "source", src.Name())

	limiter := rate.NewLimiter(rate.Limit(5), 10)
	fetchChan := make(chan sources.FetchResult)

	go src.Start(ctx, fetchChan)

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case result := <-fetchChan:
			if result.Err != nil {
				slog.Error("Source fetch error", "source", src.Name(), "err", result.Err)
				time.Sleep(backoff)
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			backoff = 1 * time.Second

			if err := limiter.Wait(ctx); err != nil {
				slog.Warn("Rate limiter error", "err", err)
				continue
			}

			for _, tick := range result.Ticks {
				p.tickChan <- tick
			}
		}
	}
}

func (p *Pipeline) dbWriter(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case tick, ok := <-p.tickChan:
			if !ok {
				return
			}
			
			p.cache.Set(tick)

			if err := p.repo.SaveTick(ctx, tick); err != nil {
				slog.Error("Failed to save tick in pipeline", "symbol", tick.Symbol, "err", err)
			}
		}
	}
}

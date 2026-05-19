package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/malharrrr/datastream/internal/ingestion"
	"github.com/malharrrr/datastream/internal/sources"
	"github.com/malharrrr/datastream/internal/storage"
	pb "github.com/malharrrr/datastream/proto"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedDataStreamServiceServer
	repo  *storage.TimescaleRepo
	cache *storage.TickCache
}

func (s *server) GetLatestTick(ctx context.Context, req *pb.GetLatestTickRequest) (*pb.Tick, error) {
	if tick := s.cache.Get(req.Symbol); tick != nil {
		return tick, nil
	}
	slog.Warn("Cache miss, falling back to DB", "symbol", req.Symbol)
	tick, err := s.repo.GetLatestTick(ctx, req.Symbol)
	if err != nil {
		return nil, err
	}

	s.cache.Set(tick)

	return tick, nil
}
func (s *server) GetTickRange(ctx context.Context, req *pb.GetTickRangeRequest) (*pb.TickRange, error) {
	ticks, err := s.repo.GetTickRange(ctx, req.Symbol, req.StartTimeMs, req.EndTimeMs)
	if err != nil {
		slog.Error("GetTickRange failed", "symbol", req.Symbol, "err", err)
		return nil, err
	}

	return &pb.TickRange{
		Symbol:    req.Symbol,
		Ticks:     ticks,
		StartTime: req.StartTimeMs,
		EndTime:   req.EndTimeMs,
	}, nil
}

func (s *server) Subscribe(req *pb.SubscribeRequest, stream pb.DataStreamService_SubscribeServer) error {
	if len(req.Symbols) == 0 {
		return fmt.Errorf("must provide at least one symbol")
	}
	symbol := req.Symbols[0]

	slog.Info("Client subscribed to stream", "symbol", symbol)
	tickChan := s.cache.Subscribe(symbol)

	defer func() {
		slog.Info("Client disconnected, cleaning up stream", "symbol", symbol)
		s.cache.Unsubscribe(symbol, tickChan)
	}()

	for {
		select {
		case <-stream.Context().Done():
			// Client dropped the connection
			return nil

		case tick := <-tickChan:
			if err := stream.Send(tick); err != nil {
				slog.Error("Failed to send tick to stream", "err", err)
				return err
			}
		}
	}
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := godotenv.Load(); err != nil {
		slog.Warn("No .env file found")
	}
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_NAME"),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo, err := storage.NewTimescaleRepo(ctx, dbURL)
	if err != nil {
		slog.Error("Failed to initialize DB", "err", err)
		os.Exit(1)
	}
	defer repo.Close()

	cache := storage.NewTickCache()
	pipeline := ingestion.NewPipeline(repo, cache, 10000)
	pipeline.AddSource(sources.NewBinanceSource("btcusdt"))
	pipeline.AddSource(sources.NewBinanceSource("ethusdt"))
	pipeline.AddSource(sources.NewYahooSource("RELIANCE.NS"))
	pipeline.AddSource(sources.NewYahooSource("TCS.NS"))
	pipeline.AddSource(sources.NewYahooSource("HDFCBANK.NS"))
	pipeline.Start(ctx)

	go func() {
		port := ":50051"
		lis, err := net.Listen("tcp", port)
		if err != nil {
			slog.Error("Failed to listen", "err", err)
			os.Exit(1)
		}
		s := grpc.NewServer()
		pb.RegisterDataStreamServiceServer(s, &server{repo: repo, cache: cache})
		slog.Info("gRPC server listening", "port", port)
		if err := s.Serve(lis); err != nil {
			slog.Error("Failed to serve", "err", err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("Shutting down gracefully...")
	cancel()
}

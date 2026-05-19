package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/malharrrr/datastream/internal/storage"
	pb "github.com/malharrrr/datastream/proto"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := godotenv.Load(); err != nil {
		slog.Warn("Could not load .env file")
	}

	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)

	ctx := context.Background()
	repo, err := storage.NewTimescaleRepo(ctx, dbURL)
	if err != nil {
		slog.Error("Database connection failed", "error", err)
		os.Exit(1)
	}
	defer repo.Close()
	slog.Info("Connected to database successfully!")

	now := time.Now()
	dummyTick := &pb.Tick{
		Symbol:       "RELIANCE",
		Exchange:     "NSE",
		TimestampMs:  now.UnixMilli(),
		Open:         2500.50,
		High:         2510.00,
		Low:          2495.00,
		Close:        2505.75,
		Volume:       15000,
		Vwap:         2503.20,
		TickCount:    150,
		DataSource:   "test_script",
		ReceivedAtMs: now.UnixMilli(),
	}

	slog.Info("Attempting to insert tick...", "symbol", dummyTick.Symbol)
	err = repo.SaveTick(ctx, dummyTick)
	if err != nil {
		slog.Error("Failed to insert tick", "error", err)
		os.Exit(1)
	}

	slog.Info("✅ Successfully inserted dummy tick into TimescaleDB!")
}
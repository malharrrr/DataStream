package storage

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/malharrrr/datastream/proto"
)

type TimescaleRepo struct {
	pool *pgxpool.Pool
}

func NewTimescaleRepo(ctx context.Context, connectionString string) (*TimescaleRepo, error) {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}
	
	config.MaxConns = 50
	config.MinConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	slog.Info("Connected to TimescaleDB")
	return &TimescaleRepo{pool: pool}, nil
}

func (r *TimescaleRepo) SaveTick(ctx context.Context, tick *pb.Tick) error {
	query := `
		INSERT INTO ticks (
			time, symbol, exchange, open, high, low, close, 
			volume, vwap, tick_count, data_source, received_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)
	`
	tickTime := time.UnixMilli(tick.TimestampMs)
	receivedAt := time.UnixMilli(tick.ReceivedAtMs)

	_, err := r.pool.Exec(ctx, query,
		tickTime, tick.Symbol, tick.Exchange, tick.Open, tick.High,
		tick.Low, tick.Close, tick.Volume, tick.Vwap, tick.TickCount,
		tick.DataSource, receivedAt,
	)

	if err != nil {
		slog.Error("Failed to insert tick", "symbol", tick.Symbol, "error", err)
		return err
	}
	return nil
}

func (r *TimescaleRepo) GetLatestTick(ctx context.Context, symbol string) (*pb.Tick, error) {
	query := `
		SELECT time, symbol, exchange, open, high, low, close, volume, vwap, tick_count, data_source, received_at
		FROM ticks
		WHERE symbol = $1
		ORDER BY time DESC
		LIMIT 1
	`
	
	row := r.pool.QueryRow(ctx, query, symbol)
	
	var t time.Time
	var receivedAt time.Time
	tick := &pb.Tick{}
	
	err := row.Scan(
		&t, &tick.Symbol, &tick.Exchange, &tick.Open, &tick.High, &tick.Low, &tick.Close, 
		&tick.Volume, &tick.Vwap, &tick.TickCount, &tick.DataSource, &receivedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest tick for %s: %w", symbol, err)
	}
	
	tick.TimestampMs = t.UnixMilli()
	tick.ReceivedAtMs = receivedAt.UnixMilli()
	return tick, nil
}

func (r *TimescaleRepo) GetTickRange(ctx context.Context, symbol string, startMs, endMs int64) ([]*pb.Tick, error) {
	query := `
		SELECT time, symbol, exchange, open, high, low, close, volume, vwap, tick_count, data_source, received_at
		FROM ticks
		WHERE symbol = $1 AND time >= $2 AND time <= $3
		ORDER BY time ASC
	`
	
	startTime := time.UnixMilli(startMs)
	endTime := time.UnixMilli(endMs)

	rows, err := r.pool.Query(ctx, query, symbol, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query tick range: %w", err)
	}
	defer rows.Close()

	var ticks []*pb.Tick
	for rows.Next() {
		var t time.Time
		var receivedAt time.Time
		tick := &pb.Tick{}
		
		err := rows.Scan(
			&t, &tick.Symbol, &tick.Exchange, &tick.Open, &tick.High, &tick.Low, &tick.Close, 
			&tick.Volume, &tick.Vwap, &tick.TickCount, &tick.DataSource, &receivedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		tick.TimestampMs = t.UnixMilli()
		tick.ReceivedAtMs = receivedAt.UnixMilli()
		ticks = append(ticks, tick)
	}
	
	return ticks, nil
}
func (r *TimescaleRepo) Close() {
	r.pool.Close()
}
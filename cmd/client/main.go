package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/malharrrr/datastream/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewDataStreamServiceClient(conn)
	ctx := context.Background()

	symbol := "TCS.NS"
	fmt.Printf("Subscribing to live trade stream for %s...\n\n", symbol)
	
	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		Symbols: []string{symbol},
	})
	if err != nil {
		log.Fatalf("Could not subscribe: %v", err)
	}
	for {
		tick, err := stream.Recv()
		if err != nil {
			log.Fatalf("Stream closed or errored: %v", err)
		}

		timeReceived := time.UnixMilli(tick.TimestampMs).Format("15:04:05.000")
		fmt.Printf("[ %s ] %s | Price: $%.2f | Vol: %d\n", 
			timeReceived, 
			tick.Symbol, 
			tick.Close, 
			tick.Volume,
		)
	}
}
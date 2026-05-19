package sources

import (
	"context"

	pb "github.com/malharrrr/datastream/proto"
)

type FetchResult struct {
	Ticks []*pb.Tick
	Err   error
}

type Source interface {
	Name() string
	Start(ctx context.Context, out chan<- FetchResult)
}
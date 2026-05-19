package storage

import (
	"sync"

	pb "github.com/malharrrr/datastream/proto"
)

type TickCache struct {
	mu    sync.RWMutex
	ticks map[string]*pb.Tick
	subs  map[string][]chan *pb.Tick 
}

func NewTickCache() *TickCache {
	return &TickCache{
		ticks: make(map[string]*pb.Tick),
		subs:  make(map[string][]chan *pb.Tick),
	}
}
func (c *TickCache) Set(tick *pb.Tick) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.ticks[tick.Symbol] = tick

	subscribers := c.subs[tick.Symbol]
	for _, ch := range subscribers {
		select {
		case ch <- tick:
		default:
		}
	}
}

func (c *TickCache) Get(symbol string) *pb.Tick {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ticks[symbol]
}

func (c *TickCache) Subscribe(symbol string) chan *pb.Tick {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan *pb.Tick, 100) 
	c.subs[symbol] = append(c.subs[symbol], ch)
	return ch
}
func (c *TickCache) Unsubscribe(symbol string, chToRemove chan *pb.Tick) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	subscribers := c.subs[symbol]
	for i, ch := range subscribers {
		if ch == chToRemove {
			c.subs[symbol] = append(subscribers[:i], subscribers[i+1:]...)
			close(ch)
			break
		}
	}
}
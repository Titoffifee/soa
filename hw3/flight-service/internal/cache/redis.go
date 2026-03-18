package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	flightTTL = 5 * time.Minute
	searchTTL = 5 * time.Minute
)

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(addr string) *RedisCache {
	client := redis.NewClient(&redis.Options{Addr: addr})
	return &RedisCache{client: client}
}

func (c *RedisCache) GetFlight(ctx context.Context, id int64) ([]byte, bool) {
	key := fmt.Sprintf("flight:%d", id)
	val, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		log.Printf("cache miss: %s", key)
		return nil, false
	}
	log.Printf("cache hit: %s", key)
	return val, true
}

func (c *RedisCache) SetFlight(ctx context.Context, id int64, data any) {
	key := fmt.Sprintf("flight:%d", id)
	b, _ := json.Marshal(data)
	c.client.Set(ctx, key, b, flightTTL)
}

func (c *RedisCache) DeleteFlight(ctx context.Context, id int64) {
	key := fmt.Sprintf("flight:%d", id)
	c.client.Del(ctx, key)
}

func (c *RedisCache) GetSearch(ctx context.Context, origin, destination, date string) ([]byte, bool) {
	key := fmt.Sprintf("search:%s:%s:%s", origin, destination, date)
	val, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		log.Printf("cache miss: %s", key)
		return nil, false
	}
	log.Printf("cache hit: %s", key)
	return val, true
}

func (c *RedisCache) SetSearch(ctx context.Context, origin, destination, date string, data any) {
	key := fmt.Sprintf("search:%s:%s:%s", origin, destination, date)
	b, _ := json.Marshal(data)
	c.client.Set(ctx, key, b, searchTTL)
}

func (c *RedisCache) InvalidateSearchByFlight(ctx context.Context, origin, destination string) {
	var pattern string
	if origin != "" && destination != "" {
		pattern = fmt.Sprintf("search:%s:%s:*", origin, destination)
	} else {
		pattern = "search:*"
	}
	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil || len(keys) == 0 {
		return
	}
	c.client.Del(ctx, keys...)
}

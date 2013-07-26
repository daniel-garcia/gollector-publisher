package main

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"encoding/json"
	"log"
)

type Metric struct {
	Name string `json:"metric"`
	Value float64 `json:"value"`
	Timestamp float64 `json:"timestamp"`
	Tags map[string]string `json:"tags"`
}

type RedisSubscriber struct {
	pool *redis.Pool
	channel string
}

func NewRedisMetricService(server string, channel string) (subscriber *RedisSubscriber, err error) {
	subscriber = &RedisSubscriber{}
	subscriber.channel = channel
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	subscriber.pool = pool
	return subscriber, err
}

func (s *RedisSubscriber) Subscribe(batchSize int) (metricChan chan []Metric) {
	metricChan = make(chan []Metric)
	go func(c chan []Metric, bs int) {
		for {
			r := s.pool.Get()
			r.Send("LRANGE", s.channel, 0, bs)
			r.Send("LTRIM", s.channel, 0, 0-bs)
			r.Flush()
			values, err := redis.Values(r.Receive())
			r.Receive()
			if err != nil {
				log.Printf("Got error: %v", err)
				continue
			}
			metricBatch := make([]Metric, len(values))
			for i, value := range(values) {
				var metric Metric
				json.Unmarshal(value.([]byte), &metric)
				metricBatch[i] = metric
			}
			c <- metricBatch
		}
	}(metricChan, batchSize - 1)
	return metricChan
}

func (s *RedisSubscriber) Publish(metric Metric) error {
	con := s.pool.Get()
	defer con.Close()
	buffer, err := json.Marshal(metric)
	_, err = con.Do("LPUSH", s.channel, buffer)
	return err
}



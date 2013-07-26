
package main

import (
	"log"
	"time"
)

func pushFake (client *RedisSubscriber, done chan bool) {
	metric := Metric{}
	metric.Name = "cpu__pct"
	metric.Value = 77.0
	metric.Tags = make(map[string]string)
	metric.Tags["uuid"] = "1231 123112 12 3 123123 1231 231231"
	for i := 0; i < 10000; i++ {
		client.Publish(metric)
	}
	done<- true
}

func main() {
	s, err := NewRedisMetricService(":6379", "metrics")
	if err != nil {
		log.Fatal("Problem creating service: %v", err)
	}

	signals := make(chan bool, 10)
	log.Printf("Starting go routines.")
	for i := 0; i < 10; i++ {
		go pushFake(s, signals)
	}
	start := time.Now()
	log.Printf("Waiting for them to finish")
	for i := 0; i < 10; i++ {
		<-signals
	}
	duration := float64(time.Now().Sub(start)) / 1000000000.0
	log.Printf("finished %f, %f",duration, 100000.0/duration)

	log.Printf("Starting subscribe!")
	c := s.Subscribe(1000)

	i := 0
	start = time.Now()
	for metrics := range(c) {
		i += len(metrics)
		if (i % 1000) == 0 {
			duration = float64(time.Now().Sub(start)) / 1000000000.0
			log.Printf("Recived %d, elapsed %f, rate %f/s", i, duration, float64(i) / duration)
		}
	}
	log.Printf("Finished!")
}


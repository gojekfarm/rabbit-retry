package main

import (
	"context"
	"github.com/gojekfarm/ziggurat/mw/event"

	"sync"

	"github.com/gojekfarm/rabbit-retry/rmq"

	"github.com/gojekfarm/ziggurat"

	"github.com/gojekfarm/ziggurat/kafka"
	"github.com/gojekfarm/ziggurat/logger"
	"github.com/gojekfarm/ziggurat/router"
)

func main() {
	l := logger.NewJSONLogger(logger.LevelInfo)
	ctx := context.Background()
	rabbitMQ := rmq.New(
		&rmq.Config{
			Hosts:       []string{"localhost:5672"},
			Username:    "user",
			Password:    "bitnami",
			QueuePrefix: "example_app",
			QueueConfig: []rmq.QueueConfig{
				{
					DelayQueueExpirationInMS: "2000",
					RouteKey:                 "plain-text-log",
					RetryCount:               2,
				},
			},
		}, rmq.WithLogger(l))

	kafkaStreams := &kafka.Streams{
		StreamConfig: kafka.StreamConfig{{
			BootstrapServers: "localhost:9092",
			ConsumerCount:    1,
			OriginTopics:     "plain-text-log",
			ConsumerGroupID:  "plain_text_consumer",
			RouteGroup:       "plain-text-log",
		}},
		Logger: l,
	}
	r := router.New()

	r.HandleFunc("plain-text-log", func(ctx context.Context, event *ziggurat.Event) error {
		return ziggurat.Retry
	})

	handler := r.Compose(rabbitMQ.Retry, event.Logger(l))

	zigKafka := &ziggurat.Ziggurat{}
	zigRabbit := &ziggurat.Ziggurat{}

	l.Error("error starting publishers", rabbitMQ.Run(ctx))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		l.Error("", zigKafka.Run(ctx, kafkaStreams, handler))
		wg.Done()
	}()
	go func() {
		l.Error("", zigRabbit.Run(ctx, rabbitMQ, handler))
		wg.Done()
	}()
	wg.Wait()
}

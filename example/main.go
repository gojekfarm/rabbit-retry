package main

import (
	"context"
	"github.com/gojekfarm/ziggurat/mw/proclog"
	"sync"

	"github.com/gojekfarm/ziggurat"
	"github.com/gojekfarm/ziggurat-rabbitmq/rmq"
	"github.com/gojekfarm/ziggurat/kafka"
	"github.com/gojekfarm/ziggurat/logger"
	"github.com/gojekfarm/ziggurat/router"
)

func main() {
	l := logger.NewJSONLogger(logger.LevelInfo)
	ctx := context.Background()
	rabbitMQ := rmq.New(
		[]string{"localhost:5672"},
		"user",
		"bitnami",
		rmq.QueueConfig{
			"plain-text-log": {
				RetryCount:               2,
				DelayQueueExpirationInMS: "500",
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

	r.HandleFunc("plain-text-log", func(ctx context.Context, event ziggurat.Event) error {
		return ziggurat.ErrProcessingFailed{Action: "retry"}
	})
	statusLogger := proclog.ProcLogger{Logger: l}
	handler := r.Compose(rabbitMQ.Retrier, statusLogger.LogStatus)

	zigKafka := &ziggurat.Ziggurat{}
	zigRabbit := &ziggurat.Ziggurat{}

	l.Error("error starting publishers", rabbitMQ.RunPublisher(ctx))

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

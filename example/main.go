package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/gojekfarm/ziggurat"
	"github.com/gojekfarm/ziggurat-rabbitmq/rmq"
	"github.com/gojekfarm/ziggurat/kafka"
	"github.com/gojekfarm/ziggurat/logger"
	"github.com/gojekfarm/ziggurat/mw"
	"github.com/gojekfarm/ziggurat/router"
)

func main() {
	l := logger.NewJSONLogger("info")
	ctx := context.Background()
	rabbitMQ := rmq.New(
		[]string{"amqp://user:bitnami@localhost:5672"},
		rmq.RabbitMQConfig{
			"plain-text-log": {
				RetryCount:               2,
				DelayQueueExpirationInMS: "500",
			},
		}, l)

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
	statusLogger := mw.ProcessingStatusLogger{Logger: l}
	handler := r.Compose(rabbitMQ.Retrier, statusLogger.LogStatus)

	zigKafka := &ziggurat.Ziggurat{}
	zigRabbit := &ziggurat.Ziggurat{}

	rabbitMQ.RunPublisher(ctx)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		err := zigKafka.Run(ctx, kafkaStreams, handler)
		fmt.Println(err)
		wg.Done()
	}()
	go func() {
		err := zigRabbit.Run(ctx, rabbitMQ, handler)
		fmt.Println(err)
		wg.Done()
	}()
	wg.Wait()
}

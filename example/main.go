package main

import (
	"context"
	"github.com/gojekfarm/ziggurat"
	"github.com/gojekfarm/ziggurat-rabbitmq/rmq"
	"github.com/gojekfarm/ziggurat/kafka"
	"github.com/gojekfarm/ziggurat/logger"
	"github.com/gojekfarm/ziggurat/mw"
	"github.com/gojekfarm/ziggurat/router"
)

func main() {
	l := logger.NewJSONLogger("info")
	rabbitMQ := rmq.New(
		[]string{"amqp://user:bitnami@localhost:5672"},
		rmq.QueueConfig{
			"plain-text-log": {
				RetryCount:               5,
				DelayQueueExpirationInMS: "500",
			},
		},
		nil)
	kafkaStreams := &kafka.Streams{
		RouteGroup: kafka.RouteGroup{
			"plain-text-log": {
				BootstrapServers: "localhost:9092",
				OriginTopics:     "plain-text-log",
				ConsumerGroupID:  "plain_text_consumer",
				ConsumerCount:    1,
			},
		},
		Logger: l,
	}
	r := router.New()
	r.HandleFunc("plain-text-log", func(event ziggurat.Event) ziggurat.ProcessStatus {
		return ziggurat.RetryMessage
	})
	statusLogger := mw.ProcessingStatusLogger{Logger: l}
	handler := r.Compose(rabbitMQ.Retrier, statusLogger.LogStatus)
	z := &ziggurat.Ziggurat{}
	z.StartFunc(func(ctx context.Context) {
		rabbitMQ.RunPublisher(ctx)
		rabbitMQ.RunConsumers(ctx, handler)
	})
	<-z.Run(context.Background(), kafkaStreams, handler)
}

//+build ignore

package main

import (
	"context"

	"github.com/gojekfarm/ziggurat/mw/event"

	"github.com/gojekfarm/rabbit-retry/rmq"

	"github.com/gojekfarm/ziggurat"

	"github.com/gojekfarm/ziggurat/kafka"
	"github.com/gojekfarm/ziggurat/logger"
	"github.com/gojekfarm/ziggurat/router"
)

func main() {
	var z ziggurat.Ziggurat
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

	kafkaStreams := kafka.Streams{
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
		return rmq.RetryErr
	})

	handler := r.Compose(rabbitMQ.Retry, event.Logger(l))

	z.StartFunc(func(ctx context.Context) {
		l.Error("error running rabbitmq", rabbitMQ.Run(ctx))
	})

	if err := z.RunAll(ctx, handler, &kafkaStreams, rabbitMQ); err != nil {
		l.Error("error running streams", err)
	}

}

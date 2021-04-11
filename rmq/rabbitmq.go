package rmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/makasim/amqpextra/consumer"

	"github.com/gojekfarm/ziggurat"
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

type Opts func(r *Retry)

type Retry struct {
	hosts          []string
	dialer         *amqpextra.Dialer
	consumerDialer *amqpextra.Dialer
	handler        ziggurat.Handler
	qconf          map[string]QueueConfig
	logger         ziggurat.StructuredLogger
	qprefix        string
}

func WithLogger(l ziggurat.StructuredLogger) Opts {
	return func(r *Retry) {
		r.logger = l
	}
}

func New(c *Config, opts ...Opts) *Retry {

	r := &Retry{
		hosts:   c.generateAMQPURLS(),
		qconf:   c.transformQueueConfig(),
		qprefix: c.getQPrefix(),
	}

	for _, opt := range opts {
		opt(r)
	}
	if r.logger == nil {
		r.logger = LoggerFunc(func() {})
	}
	return r
}

func (r *Retry) Handle(ctx context.Context, event ziggurat.Event) error {
	err, ok := (r.handler.Handle(ctx, event)).(ziggurat.ErrProcessingFailed)
	if ok && err.Action == "retry" {
		r.logger.Info("rabbitmq retrying message")
		err := r.retry(ctx, event)
		r.logger.Error("error retrying message", err)
	}
	return err
}

func (r *Retry) Retrier(handler ziggurat.Handler) ziggurat.Handler {
	f := ziggurat.HandlerFunc(func(ctx context.Context, messageEvent ziggurat.Event) error {
		if r.dialer == nil {
			panic("dialer nil error: run the `RunPublisher` method")
		}

		err, ok := (handler.Handle(ctx, messageEvent)).(ziggurat.ErrProcessingFailed)
		if ok && err.Action == "retry" {
			r.logger.Info("rabbitmq retrying message")
			retryErr := r.retry(ctx, messageEvent)
			r.logger.Error("error retrying message", retryErr)
		}
		return err
	})
	return f
}

func (r *Retry) RunPublisher(ctx context.Context) error {
	return r.initPublisher(ctx)
}

func (r *Retry) Stream(ctx context.Context, handler ziggurat.Handler) error {
	consumerDialer, dialErr := amqpextra.NewDialer(
		amqpextra.WithContext(ctx),
		amqpextra.WithURL(r.hosts...))
	if dialErr != nil {
		return dialErr
	}
	var wg sync.WaitGroup
	r.consumerDialer = consumerDialer
	consumers := make([]*consumer.Consumer, 0, len(r.qconf))
	for routeName := range r.qconf {
		queueName := constructQueueName(routeName, "instant", r.qprefix)
		ctag := fmt.Sprintf("%s_%s_%s", queueName, r.qprefix, "ctag")
		r.logger.Info("creating rabbitmq consumer", map[string]interface{}{"ctag": ctag, "queue-name": queueName})
		c, err := createConsumer(ctx, r.consumerDialer, ctag, queueName, handler, r.logger)
		if err != nil {
			return err
		}
		consumers = append(consumers, c)
		wg.Add(1)
		go func() {
			<-c.NotifyClosed()
			r.logger.Info("shutting down rabbitmq consumer", map[string]interface{}{"ctag": ctag})
			wg.Done()
		}()
		if len(consumers) != len(r.qconf) {
			for _, c := range consumers {
				c.Close()
			}
			return errors.New("error: couldn't start all consumers")
		}
		wg.Wait()
	}
	return nil
}

func (r *Retry) retry(ctx context.Context, event ziggurat.Event) error {
	pub, pubCreateError := r.dialer.Publisher(publisher.WithContext(ctx))
	if pubCreateError != nil {
		return pubCreateError
	}
	defer pub.Close()

	publishing := amqp.Publishing{}
	message := publisher.Message{}

	routeName := event.Headers()[ziggurat.HeaderMessageRoute]
	var payload RabbitMQPayload
	if eventCast, ok := event.(RabbitMQPayload); ok {
		payload = eventCast
	} else {
		payload = RabbitMQPayload{
			MessageVal:     event.Value(),
			MessageHeaders: event.Headers(),
			RetryCount:     0,
		}
	}

	if payload.RetryCount >= r.qconf[routeName].RetryCount {
		message.Exchange = constructExchangeName(routeName, "dead_letter", r.qprefix)
		publishing.Expiration = ""
	} else {
		message.Exchange = constructExchangeName(routeName, "delay", r.qprefix)
		publishing.Expiration = r.qconf[routeName].DelayQueueExpirationInMS
		payload.RetryCount = payload.RetryCount + 1
	}

	buff, err := encodeMessage(payload)
	if err != nil {
		return err
	}

	publishing.Body = buff.Bytes()
	message.Publishing = publishing
	return pub.Publish(message)
}

func (r *Retry) initPublisher(ctx context.Context) error {
	ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, 30*time.Second)
	args := map[string]interface{}{"hosts": strings.Join(r.hosts, ",")}
	r.logger.Info("dialing rabbitmq server", args)
	go func() {
		<-ctxWithTimeout.Done()
		cancelFunc()
	}()
	dialer, cfgErr := amqpextra.NewDialer(
		amqpextra.WithContext(ctx),
		amqpextra.WithURL(r.hosts...))
	if cfgErr != nil {
		return cfgErr
	}
	r.dialer = dialer

	conn, connErr := r.dialer.Connection(ctxWithTimeout)
	if connErr != nil {
		return connErr
	}
	defer conn.Close()

	channel, chanErr := conn.Channel()
	if chanErr != nil {
		return chanErr
	}
	defer channel.Close()

	queueTypes := []string{"instant", "delay", "dead_letter"}
	for _, queueType := range queueTypes {
		var args amqp.Table
		for route := range r.qconf {
			if queueType == "delay" {
				args = amqp.Table{"x-dead-letter-exchange": constructExchangeName(route, "instant", r.qprefix)}
			}
			exchangeName := constructExchangeName(route, queueType, r.qprefix)
			if exchangeDeclareErr := channel.ExchangeDeclare(exchangeName, amqp.ExchangeFanout, true, false, false, false, nil); exchangeDeclareErr != nil {
				return exchangeDeclareErr
			}

			queueName := constructQueueName(route, queueType, r.qprefix)
			if _, queueDeclareErr := channel.QueueDeclare(queueName, true, false, false, false, args); queueDeclareErr != nil {
				return queueDeclareErr
			}

			if bindErr := channel.QueueBind(queueName, "", exchangeName, false, args); bindErr != nil {
				return bindErr
			}
		}
	}
	return nil
}

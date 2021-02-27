package rmq

import (
	"context"
	"fmt"
	"github.com/gojekfarm/ziggurat"
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
	"strings"
	"time"
)

type RabbitMQConfig map[string]struct {
	RetryCount               int
	DelayQueueExpirationInMS string
}

type Retry struct {
	hosts          []string
	dialer         *amqpextra.Dialer
	consumerDialer *amqpextra.Dialer
	handler        ziggurat.Handler
	config         RabbitMQConfig
	logger         ziggurat.StructuredLogger
}

func New(hosts []string, queueConfig RabbitMQConfig, logger ziggurat.StructuredLogger) *Retry {
	r := &Retry{
		hosts:  hosts,
		config: queueConfig,
		logger: logger,
	}
	if r.logger == nil {
		r.logger = LoggerFunc(func() {})
	}
	return r
}

func (r *Retry) HandleEvent(event ziggurat.Event, ctx context.Context) error {
	err, ok := (r.handler.HandleEvent(ctx, event)).(ziggurat.ErrProcessingFailed)
	if ok && err.Action == "retry" {
		err := r.retry(event, ctx)
		r.logger.Error("error retrying message", err)
	}
	return err
}

func (r *Retry) Retrier(handler ziggurat.Handler) ziggurat.Handler {
	return ziggurat.HandlerFunc(func(ctx context.Context, messageEvent ziggurat.Event) error {
		if r.dialer == nil {
			panic("dialer nil error: run the `RunPublisher` method")
		}

		err, ok := (handler.HandleEvent(ctx, messageEvent)).(ziggurat.ErrProcessingFailed)
		if ok && err.Action == "retry" {
			retryErr := r.retry(messageEvent, ctx)
			r.logger.Error("error retrying message", retryErr)
		}
		return err
	})
}

func (r *Retry) RunPublisher(ctx context.Context) error {
	return r.initPublisher(ctx)
}

func (r *Retry) Stream(ctx context.Context, handler ziggurat.Handler) chan error {
	consumerDialer, dialErr := amqpextra.NewDialer(
		amqpextra.WithContext(ctx),
		amqpextra.WithURL(r.hosts...))
	errChan := make(chan error)
	if dialErr != nil {
		go func() {
			errChan <- dialErr
		}()
		return errChan
	}
	r.consumerDialer = consumerDialer
	consumerCloseChans := make([]<-chan struct{}, 0, len(r.config))
	for routeName, _ := range r.config {
		queueName := constructQueueName(routeName, "instant")
		ctag := fmt.Sprintf("%s_%s_%s", queueName, "ziggurat", "ctag")
		c, err := createConsumer(ctx, r.consumerDialer, ctag, queueName, handler, r.logger)
		if err != nil {
			go func() {
				errChan <- err
			}()
			return errChan
		}
		consumerCloseChans = append(consumerCloseChans, c.NotifyClosed())
		go func() {
			for _, c := range consumerCloseChans {
				<-c
			}
			errChan <- nil
		}()
	}
	return errChan
}

func (r *Retry) retry(event ziggurat.Event, ctx context.Context) error {
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

	if payload.RetryCount >= r.config[routeName].RetryCount {
		message.Exchange = constructExchangeName(routeName, "dead_letter")
		publishing.Expiration = ""
	} else {
		message.Exchange = constructExchangeName(routeName, "delay")
		publishing.Expiration = r.config[routeName].DelayQueueExpirationInMS
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
		for route, _ := range r.config {
			if queueType == "delay" {
				args = amqp.Table{"x-dead-letter-exchange": constructExchangeName(route, "instant")}
			}
			exchangeName := constructExchangeName(route, queueType)
			if exchangeDeclareErr := channel.ExchangeDeclare(exchangeName, amqp.ExchangeFanout, true, false, false, false, nil); exchangeDeclareErr != nil {
				return exchangeDeclareErr
			}

			queueName := constructQueueName(route, queueType)
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

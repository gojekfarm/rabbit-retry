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

type QueueConfig map[string]*struct {
	RetryCount               int
	DelayQueueExpirationInMS string
}

type Retry struct {
	hosts          []string
	dialer         *amqpextra.Dialer
	consumerDialer *amqpextra.Dialer
	handler        ziggurat.Handler
	queueConfig    QueueConfig
	logger         ziggurat.StructuredLogger
}

func New(hosts []string, queueConfig QueueConfig, logger ziggurat.StructuredLogger) *Retry {
	r := &Retry{
		hosts:       hosts,
		queueConfig: queueConfig,
		logger:      logger,
	}
	if r.logger == nil {
		r.logger = LoggerFunc(func() {})
	}
	return r
}

func (r *Retry) HandleEvent(event ziggurat.Event) ziggurat.ProcessStatus {
	status := r.handler.HandleEvent(event)
	if status == ziggurat.RetryMessage {
		err := r.retry(event)
		r.logger.Error("error retrying message", err)
	}
	return status
}

func (r *Retry) Retrier(handler ziggurat.Handler) ziggurat.Handler {
	return ziggurat.HandlerFunc(func(messageEvent ziggurat.Event) ziggurat.ProcessStatus {
		if r.dialer == nil {
			panic("dialer nil error: run the `RunPublisher` method")
		}
		status := handler.HandleEvent(messageEvent)
		if status == ziggurat.RetryMessage {
			err := r.retry(messageEvent)
			r.logger.Error("error retrying message", err)
		}
		return status
	})
}

func (r *Retry) RunPublisher(ctx context.Context) error {
	return r.initPublisher(ctx)
}

func (r *Retry) RunConsumers(ctx context.Context, handler ziggurat.Handler) error {
	consumerDialer, dialErr := amqpextra.NewDialer(
		amqpextra.WithContext(ctx),
		amqpextra.WithURL(r.hosts...))
	if dialErr != nil {
		return dialErr
	}
	r.consumerDialer = consumerDialer
	for routeName, _ := range r.queueConfig {
		queueName := constructQueueName(routeName, "instant")
		ctag := fmt.Sprintf("%s_%s_%s", queueName, "ziggurat", "ctag")
		c, err := createConsumer(ctx, r.consumerDialer, ctag, queueName, handler, r.logger)
		if err != nil {

		}
		go func() {
			<-c.NotifyClosed()
			r.logger.Error("consumer closed", nil)
		}()
	}
	return nil
}

func (r *Retry) retry(event ziggurat.Event) error {
	pub, pubCreateError := r.dialer.Publisher(publisher.WithContext(event.Context()))
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
			ctx:            event.Context(),
			RetryCount:     0,
		}
	}

	if payload.RetryCount >= r.queueConfig[string(routeName)].RetryCount {
		message.Exchange = constructExchangeName(routeName, "dead_letter")
		publishing.Expiration = ""
	} else {
		message.Exchange = constructExchangeName(routeName, "delay")
		publishing.Expiration = r.queueConfig[routeName].DelayQueueExpirationInMS
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
		for route, _ := range r.queueConfig {
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

//func (r *Retry) Stream(ctx context.Context, handler ziggurat.Handler) chan error {
//	errChan := make(chan error)
//	err := r.RunConsumers(ctx, handler)
//	if err != nil {
//		go func() {
//			errChan <- err
//		}()
//		return errChan
//	}
//	return errChan
//}

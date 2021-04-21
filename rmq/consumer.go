package rmq

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"github.com/gojekfarm/ziggurat"
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
)

var decodeMessage = func(body []byte) (*ziggurat.Event, error) {
	buff := bytes.Buffer{}
	buff.Write(body)
	decoder := gob.NewDecoder(&buff)
	var event ziggurat.Event
	if decodeErr := decoder.Decode(&event); decodeErr != nil {
		return nil, decodeErr
	}

	return &event, nil
}

var createConsumer = func(ctx context.Context, d *amqpextra.Dialer, ctag string, queueName string, msgHandler ziggurat.Handler, l ziggurat.StructuredLogger) (*consumer.Consumer, error) {
	options := []consumer.Option{
		consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
			channel, err := conn.(*amqp.Connection).Channel()
			if err != nil {
				return nil, err
			}
			l.Error("rabbitmq: error setting QOS", channel.Qos(1, 0, false))
			return channel, nil
		}),
		consumer.WithContext(ctx),
		consumer.WithConsumeArgs(ctag, false, false, false, false, nil),
		consumer.WithQueue(queueName),
		consumer.WithHandler(consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			l.Info("rabbitmq processing message from queue", map[string]interface{}{"queue-name": queueName})
			msgEvent, err := decodeMessage(msg.Body)
			if err != nil {
				l.Error("error decoding message", err)
				return msg.Reject(true)
			}
			msgEvent.EventType = "amqp"
			msgEvent.ProducerTimestamp = msg.Timestamp
			msgEvent.ReceivedTimestamp = time.Now()
			l.Error("error processing amqp message", msgHandler.Handle(ctx, msgEvent))
			return msg.Ack(false)
		}))}
	return d.Consumer(options...)
}

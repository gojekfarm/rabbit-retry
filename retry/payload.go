package retry

import (
	"context"
)

type RabbitMQPayload struct {
	MessageVal     []byte
	MessageHeaders map[string]string
	ctx            context.Context
	RetryCount     int
}

func (r RabbitMQPayload) Value() []byte {
	return r.MessageVal
}

func (r RabbitMQPayload) Headers() map[string]string {
	return r.MessageHeaders
}

func (r RabbitMQPayload) Context() context.Context {
	return r.ctx
}

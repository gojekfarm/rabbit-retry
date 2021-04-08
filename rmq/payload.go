package rmq

type RabbitMQPayload struct {
	MessageVal     []byte
	MessageHeaders map[string]string
	RetryCount     int
	MessageKey     []byte
}

func (r RabbitMQPayload) Value() []byte {
	return r.MessageVal
}

func (r RabbitMQPayload) Headers() map[string]string {
	return r.MessageHeaders
}

func (r RabbitMQPayload) Key() []byte {
	return r.MessageKey
}

package retry

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
)

func constructQueueName(routeName string, queueType string) string {
	return fmt.Sprintf("%s_%s_%s_queue", "ziggurat", routeName, queueType)
}

func constructExchangeName(route string, queueType string) string {
	return fmt.Sprintf("%s_%s_%s_exchange", "ziggurat", route, queueType)
}

func encodeMessage(message RabbitMQPayload) (*bytes.Buffer, error) {

	buff := bytes.NewBuffer([]byte{})
	gob.Register(time.Time{})
	encoder := gob.NewEncoder(buff)

	if err := encoder.Encode(message); err != nil {
		return nil, err
	}
	return buff, nil
}

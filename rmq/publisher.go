package rmq

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/gojekfarm/ziggurat"
)

func constructQueueName(routeName string, queueType string, prefix string) string {
	return fmt.Sprintf("%s_%s_%s_queue", prefix, routeName, queueType)
}

func constructExchangeName(route string, queueType string, prefix string) string {
	return fmt.Sprintf("%s_%s_%s_exchange", prefix, route, queueType)
}

func encodeMessage(event *ziggurat.Event) (*bytes.Buffer, error) {

	buff := bytes.NewBuffer([]byte{})
	gob.Register(time.Time{})
	encoder := gob.NewEncoder(buff)

	if err := encoder.Encode(event); err != nil {
		return nil, err
	}
	return buff, nil
}

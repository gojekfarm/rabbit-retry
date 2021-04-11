package rmq

import "fmt"

type QueueConfig struct {
	RetryCount               int
	DelayQueueExpirationInMS string
	RouteKey                 string
}

type Config struct {
	Hosts       []string
	Username    string
	Password    string
	QueuePrefix string
	QueueConfig []QueueConfig
}

func (c *Config) generateAMQPURLS() []string {
	amqpFmtURLs := make([]string, 0, len(c.Hosts))

	for _, host := range c.Hosts {
		url := fmt.Sprintf("amqp://%s:%s@%s", c.Username, c.Password, host)
		amqpFmtURLs = append(amqpFmtURLs, url)
	}
	return amqpFmtURLs
}

func (c *Config) transformQueueConfig() map[string]QueueConfig {
	qconfMap := make(map[string]QueueConfig, len(c.QueueConfig))
	for _, qconf := range c.QueueConfig {
		qconfMap[qconf.RouteKey] = qconf
	}
	return qconfMap
}

func (c *Config) validate() {
	if len(c.Hosts) == 0 {
		panic("hosts cannot be empty")
	}
	if len(c.QueueConfig) == 0 {
		panic("queue config cannot be empty")
	}
	if c.Username == "" || c.Password == "" {
		panic("username OR password cannot be empty")
	}

	for _, q := range c.QueueConfig {
		if q.RouteKey == "" {
			panic("`QueueConfig.RouteKey` cannot be empty")
		}
	}

}

func (c *Config) getQPrefix() string {
	if c.QueuePrefix == "" {
		return "ziggurat"
	}
	return c.QueuePrefix
}

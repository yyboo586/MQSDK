package mqsd

import (
	"fmt"
)

// Factory 消息队列工厂
type Factory struct{}

// NewFactory 创建工厂实例
func NewFactory() *Factory {
	return &Factory{}
}

// NewProducer 创建生产者
func (f *Factory) NewProducer(config Config) (Producer, error) {
	switch config.GetType() {
	case "nsq":
		if nsqConfig, ok := config.(*NSQConfig); ok {
			return NewNSQProducer(nsqConfig)
		}
		return nil, fmt.Errorf("invalid NSQ config")
	case "kafka":
		if kafkaConfig, ok := config.(*KafkaConfig); ok {
			return NewKafkaProducer(kafkaConfig)
		}
		return nil, fmt.Errorf("invalid Kafka config")
	case "rabbitmq":
		if rabbitConfig, ok := config.(*RabbitMQConfig); ok {
			return NewRabbitMQProducer(rabbitConfig)
		}
		return nil, fmt.Errorf("invalid RabbitMQ config")
	default:
		return nil, fmt.Errorf("unsupported message queue type: %s", config.GetType())
	}
}

// NewConsumer 创建消费者
func (f *Factory) NewConsumer(config Config) (Consumer, error) {
	switch config.GetType() {
	case "nsq":
		if nsqConfig, ok := config.(*NSQConfig); ok {
			return NewNSQConsumer(nsqConfig)
		}
		return nil, fmt.Errorf("invalid NSQ config")
	case "kafka":
		if kafkaConfig, ok := config.(*KafkaConfig); ok {
			return NewKafkaConsumer(kafkaConfig)
		}
		return nil, fmt.Errorf("invalid Kafka config")
	case "rabbitmq":
		if rabbitConfig, ok := config.(*RabbitMQConfig); ok {
			return NewRabbitMQConsumer(rabbitConfig)
		}
		return nil, fmt.Errorf("invalid RabbitMQ config")
	default:
		return nil, fmt.Errorf("unsupported message queue type: %s", config.GetType())
	}
}

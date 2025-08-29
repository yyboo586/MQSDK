package mqsd

import (
	"context"
)

// Message 消息结构体
type Message struct {
	ID        string                 `json:"id"`
	Topic     string                 `json:"topic"`
	Body      interface{}            `json:"body"`
	Headers   map[string]interface{} `json:"headers,omitempty"`
	Timestamp int64                  `json:"timestamp"`
}

// MessageHandler 消息处理函数类型
type MessageHandler func(msg *Message) error

// Producer 生产者接口
type Producer interface {
	// Publish 发布消息
	Publish(ctx context.Context, topic string, msg *Message) error
	// Close 关闭连接
	Close() error
}

// Consumer 消费者接口
type Consumer interface {
	// Subscribe 订阅主题
	Subscribe(ctx context.Context, topic string, channel string, handler MessageHandler) error
	// Unsubscribe 取消订阅
	Unsubscribe(topic string) error
	// Close 关闭连接
	Close() error
}

// Config 配置接口
type Config interface {
	GetType() string
}

// NSQConfig NSQ配置
type NSQConfig struct {
	Type      string   `json:"type"`
	NSQDAddr  string   `json:"nsqd_addr"`
	NSQLookup []string `json:"nsqlookup_addrs"`
}

func (c *NSQConfig) GetType() string {
	return "nsq"
}

// KafkaConfig Kafka配置
type KafkaConfig struct {
	Type    string   `json:"type"`
	Brokers []string `json:"brokers"`
	GroupID string   `json:"group_id"`
	Version string   `json:"version"`
}

func (c *KafkaConfig) GetType() string {
	return "kafka"
}

// RabbitMQConfig RabbitMQ配置
type RabbitMQConfig struct {
	Type     string `json:"type"`
	URL      string `json:"url"`
	Exchange string `json:"exchange"`
}

func (c *RabbitMQConfig) GetType() string {
	return "rabbitmq"
}

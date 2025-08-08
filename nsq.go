package mqsd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/nsqio/go-nsq"
)

// NSQProducer NSQ生产者
type NSQProducer struct {
	producer *nsq.Producer
	config   *NSQConfig
}

// NewNSQProducer 创建NSQ生产者
func NewNSQProducer(config *NSQConfig) (*NSQProducer, error) {
	producer, err := nsq.NewProducer(config.NSQDAddr, nsq.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create NSQ producer: %w", err)
	}

	return &NSQProducer{
		producer: producer,
		config:   config,
	}, nil
}

// Publish 发布消息
func (p *NSQProducer) Publish(ctx context.Context, topic string, msg *Message) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().Unix()
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	return p.producer.Publish(topic, data)
}

// Close 关闭连接
func (p *NSQProducer) Close() error {
	p.producer.Stop()
	return nil
}

// NSQConsumer NSQ消费者
type NSQConsumer struct {
	config    *NSQConfig
	consumers map[string]*nsq.Consumer
	handlers  map[string]MessageHandler
}

// NewNSQConsumer 创建NSQ消费者
func NewNSQConsumer(config *NSQConfig) (*NSQConsumer, error) {
	nsqConsumer := &NSQConsumer{
		config:    config,
		handlers:  make(map[string]MessageHandler),
		consumers: make(map[string]*nsq.Consumer),
	}

	return nsqConsumer, nil
}

// Subscribe 订阅主题
func (c *NSQConsumer) Subscribe(ctx context.Context, topic string, channel string, handler MessageHandler) error {
	c.handlers[topic] = handler

	// 为每个topic创建独立的Consumer
	if err := c.createConsumerForTopic(topic, channel); err != nil {
		return fmt.Errorf("failed to create consumer for topic %s: %w", topic, err)
	}

	return nil
}

// createConsumerForTopic 为指定topic创建Consumer
func (c *NSQConsumer) createConsumerForTopic(topic string, channel string) error {
	// 检查是否已经为这个topic创建了Consumer
	if _, exists := c.consumers[topic]; exists {
		return nil // 已经存在，不需要重复创建
	}

	nsqConfig := nsq.NewConfig()
	nsqConfig.MaxInFlight = 10

	consumer, err := nsq.NewConsumer(topic, channel, nsqConfig)
	if err != nil {
		return fmt.Errorf("failed to create NSQ consumer: %w", err)
	}

	// 为这个Consumer设置handler
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		var msg Message
		if err := json.Unmarshal(message.Body, &msg); err != nil {
			log.Printf("failed to unmarshal message: %v", err)
			return err
		}

		// 使用当前topic的handler
		if handler, exists := c.handlers[topic]; exists {
			return handler(&msg)
		}

		return nil
	}))

	// 连接到NSQLookup或直接连接到NSQD
	var err2 error
	if len(c.config.NSQLookup) > 0 {
		err2 = consumer.ConnectToNSQLookupd(c.config.NSQLookup[0])
	} else {
		err2 = consumer.ConnectToNSQD(c.config.NSQDAddr)
	}

	if err2 != nil {
		return fmt.Errorf("failed to connect to NSQ: %w", err2)
	}

	// 保存Consumer实例
	c.consumers[topic] = consumer

	return nil
}

// Unsubscribe 取消订阅
func (c *NSQConsumer) Unsubscribe(topic string) error {
	delete(c.handlers, topic)
	return nil
}

// Close 关闭连接
func (c *NSQConsumer) Close() error {
	// 关闭所有Consumer
	for topic, consumer := range c.consumers {
		consumer.Stop()
		log.Printf("Stopped consumer for topic: %s", topic)
	}
	return nil
}

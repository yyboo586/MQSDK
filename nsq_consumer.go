package mqsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

// NSQConsumer NSQ消费者
type NSQConsumer struct {
	config    *NSQConfig
	consumers map[string]*nsq.Consumer
	handlers  map[string]MessageHandler

	mu sync.RWMutex
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
	c.mu.Lock()
	c.handlers[topic] = handler
	c.mu.Unlock()

	// 为每个topic创建独立的Consumer
	if err := c.createConsumerForTopic(topic, channel); err != nil {
		return fmt.Errorf("failed to create consumer for topic %s: %w", topic, err)
	}
	return nil
}

// createConsumerForTopic 为指定topic创建Consumer
func (c *NSQConsumer) createConsumerForTopic(topic string, channel string) error {
	c.mu.Lock()
	if _, exists := c.consumers[topic]; exists {
		c.mu.Unlock()
		return nil
	}

	nsqConfig := nsq.NewConfig()
	nsqConfig.MaxInFlight = 10
	nsqConfig.MaxRequeueDelay = time.Second * 90
	nsqConfig.DefaultRequeueDelay = time.Second * 15

	consumer, err := nsq.NewConsumer(topic, channel, nsqConfig)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("failed to create NSQ consumer: %w", err)
	}
	c.mu.Unlock()

	// 为这个Consumer设置handler
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		var msg Message
		if err := json.Unmarshal(message.Body, &msg); err != nil {
			log.Printf("failed to unmarshal message: %v", err)
			return err
		}

		// 使用当前topic的handler
		c.mu.RLock()
		if handler, exists := c.handlers[topic]; exists {
			c.mu.RUnlock()
			return handler(&msg)
		}
		c.mu.RUnlock()

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

	c.mu.Lock()
	c.consumers[topic] = consumer
	c.mu.Unlock()

	return nil
}

// Unsubscribe 取消订阅
func (c *NSQConsumer) Unsubscribe(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.handlers, topic)
	return nil
}

// Close 关闭连接
func (c *NSQConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 关闭所有Consumer
	for topic, consumer := range c.consumers {
		consumer.Stop()
		log.Printf("Stopped consumer for topic: %s", topic)
	}
	return nil
}

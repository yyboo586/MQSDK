package mqsdk

import (
	"context"
	"encoding/json"
	"fmt"
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

	err = p.producer.Publish(topic, data)
	if err != nil {
		fmt.Printf("[NSQ ERROR] Failed to publish message to topic: %s, msgID: %s, error: %v\n", topic, msg.ID, err)
		return err
	}
	
	fmt.Printf("[NSQ] Successfully published message to topic: %s, msgID: %s, timestamp: %d\n", topic, msg.ID, msg.Timestamp)
	return nil
}

// Close 关闭连接
func (p *NSQProducer) Close() error {
	p.producer.Stop()
	return nil
}

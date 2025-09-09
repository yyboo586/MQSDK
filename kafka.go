package mqsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

// KafkaProducer Kafka生产者
type KafkaProducer struct {
	producer sarama.SyncProducer
	config   *KafkaConfig
}

// NewKafkaProducer 创建Kafka生产者
func NewKafkaProducer(config *KafkaConfig) (*KafkaProducer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &KafkaProducer{
		producer: producer,
		config:   config,
	}, nil
}

// Publish 发布消息
func (p *KafkaProducer) Publish(ctx context.Context, topic string, msg *Message) error {
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

	kafkaMsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
		Headers: []sarama.RecordHeader{
			{Key: []byte("message_id"), Value: []byte(msg.ID)},
		},
	}

	_, _, err = p.producer.SendMessage(kafkaMsg)
	return err
}

// Close 关闭连接
func (p *KafkaProducer) Close() error {
	return p.producer.Close()
}

// KafkaConsumer Kafka消费者
type KafkaConsumer struct {
	consumer sarama.ConsumerGroup
	config   *KafkaConfig
	handlers map[string]MessageHandler
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewKafkaConsumer 创建Kafka消费者
func NewKafkaConsumer(config *KafkaConfig) (*KafkaConsumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	kafkaConsumer := &KafkaConsumer{
		consumer: consumer,
		config:   config,
		handlers: make(map[string]MessageHandler),
		ctx:      ctx,
		cancel:   cancel,
	}

	return kafkaConsumer, nil
}

// Subscribe 订阅主题
func (c *KafkaConsumer) Subscribe(ctx context.Context, topic string, channel string, handler MessageHandler) error {
	c.handlers[topic] = handler

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				err := c.consumer.Consume(c.ctx, []string{topic}, c)
				if err != nil {
					log.Printf("Error from consumer: %v", err)
				}
			}
		}
	}()

	return nil
}

// Unsubscribe 取消订阅
func (c *KafkaConsumer) Unsubscribe(topic string) error {
	delete(c.handlers, topic)
	return nil
}

// Close 关闭连接
func (c *KafkaConsumer) Close() error {
	c.cancel()
	return c.consumer.Close()
}

// ConsumeClaim 实现sarama.ConsumerGroupHandler接口
func (c *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			var msg Message
			if err := json.Unmarshal(message.Value, &msg); err != nil {
				log.Printf("failed to unmarshal message: %v", err)
				continue
			}

			if handler, exists := c.handlers[message.Topic]; exists {
				if err := handler(&msg); err != nil {
					log.Printf("failed to handle message: %v", err)
				}
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup 实现sarama.ConsumerGroupHandler接口
func (c *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 实现sarama.ConsumerGroupHandler接口
func (c *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

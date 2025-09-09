package mqsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// RabbitMQProducer RabbitMQ生产者
type RabbitMQProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *RabbitMQConfig
}

// NewRabbitMQProducer 创建RabbitMQ生产者
func NewRabbitMQProducer(config *RabbitMQConfig) (*RabbitMQProducer, error) {
	conn, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// 声明交换机
	err = channel.ExchangeDeclare(
		config.Exchange, // name
		"topic",         // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	return &RabbitMQProducer{
		conn:    conn,
		channel: channel,
		config:  config,
	}, nil
}

// Publish 发布消息
func (p *RabbitMQProducer) Publish(ctx context.Context, topic string, msg *Message) error {
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

	return p.channel.Publish(
		p.config.Exchange, // exchange
		topic,             // routing key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
			Headers: amqp.Table{
				"message_id": msg.ID,
			},
		},
	)
}

// Close 关闭连接
func (p *RabbitMQProducer) Close() error {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// RabbitMQConsumer RabbitMQ消费者
type RabbitMQConsumer struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	config   *RabbitMQConfig
	handlers map[string]MessageHandler
	queues   map[string]string
}

// NewRabbitMQConsumer 创建RabbitMQ消费者
func NewRabbitMQConsumer(config *RabbitMQConfig) (*RabbitMQConsumer, error) {
	conn, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// 声明交换机
	err = channel.ExchangeDeclare(
		config.Exchange, // name
		"topic",         // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	return &RabbitMQConsumer{
		conn:     conn,
		channel:  channel,
		config:   config,
		handlers: make(map[string]MessageHandler),
		queues:   make(map[string]string),
	}, nil
}

// Subscribe 订阅主题
func (c *RabbitMQConsumer) Subscribe(ctx context.Context, topic string, channel string, handler MessageHandler) error {
	c.handlers[topic] = handler

	// 声明队列
	queue, err := c.channel.QueueDeclare(
		"",    // name (空字符串表示随机队列名)
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// 绑定队列到交换机
	err = c.channel.QueueBind(
		queue.Name,        // queue name
		topic,             // routing key
		c.config.Exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	c.queues[topic] = queue.Name

	// 开始消费消息
	msgs, err := c.channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	go func() {
		for msg := range msgs {
			var message Message
			if err := json.Unmarshal(msg.Body, &message); err != nil {
				log.Printf("failed to unmarshal message: %v", err)
				continue
			}

			if err := handler(&message); err != nil {
				log.Printf("failed to handle message: %v", err)
			}
		}
	}()

	return nil
}

// Unsubscribe 取消订阅
func (c *RabbitMQConsumer) Unsubscribe(topic string) error {
	delete(c.handlers, topic)
	if queueName, exists := c.queues[topic]; exists {
		c.channel.QueueDelete(queueName, false, false, false)
		delete(c.queues, topic)
	}
	return nil
}

// Close 关闭连接
func (c *RabbitMQConsumer) Close() error {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

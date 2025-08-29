package main

import (
	"context"
	"fmt"
	"log"
	"time"

	mqsd "github.com/yyboo586/MQSDK"
)

func main() {
	// 创建RabbitMQ配置
	config := &mqsd.RabbitMQConfig{
		Type:     "rabbitmq",
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test-exchange",
	}

	// 创建工厂
	factory := mqsd.NewFactory()

	// 创建生产者
	producer, err := factory.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// 创建消费者
	consumer, err := factory.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// 订阅主题
	err = consumer.Subscribe(context.Background(), "test-topic", "test-channel", func(msg *mqsd.Message) error {
		fmt.Printf("Received message: ID=%s, Topic=%s, Body=%s\n",
			msg.ID, msg.Topic, msg.Body)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// 发布消息
	message := &mqsd.Message{
		Topic: "test-topic",
		Body:  []byte("Hello RabbitMQ!"),
		Headers: map[string]interface{}{
			"source": "example",
		},
	}

	err = producer.Publish(context.Background(), "test-topic", message)
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}

	fmt.Println("Message published successfully")

	// 等待一段时间让消息被消费
	time.Sleep(2 * time.Second)
}

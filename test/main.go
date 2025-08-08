package main

import (
	"fmt"

	mqsd "github.com/yyboo586/MQSDK"
)

func main() {
	// 测试NSQ配置
	nsqConfig := &mqsd.NSQConfig{
		Type:      "nsq",
		NSQDAddr:  "localhost:4150",
		NSQLookup: []string{"localhost:4161"},
	}

	// 测试Kafka配置
	kafkaConfig := &mqsd.KafkaConfig{
		Type:    "kafka",
		Brokers: []string{"localhost:9092"},
		GroupID: "test-group",
		Version: "2.8.0",
	}

	// 测试RabbitMQ配置
	rabbitConfig := &mqsd.RabbitMQConfig{
		Type:     "rabbitmq",
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "test-exchange",
	}

	// 创建工厂
	_ = mqsd.NewFactory()

	fmt.Printf("NSQ Config Type: %s\n", nsqConfig.GetType())
	fmt.Printf("Kafka Config Type: %s\n", kafkaConfig.GetType())
	fmt.Printf("RabbitMQ Config Type: %s\n", rabbitConfig.GetType())

	// 测试消息创建
	message := &mqsd.Message{
		ID:    "test-id",
		Topic: "test-topic",
		Body:  "Hello World",
		Headers: map[string]string{
			"source": "test",
		},
	}

	fmt.Printf("Message created: ID=%s, Topic=%s, Body=%s\n",
		message.ID, message.Topic, message.Body)

	fmt.Println("Build test completed successfully!")
}

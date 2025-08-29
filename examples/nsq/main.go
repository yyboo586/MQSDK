package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	mqsd "github.com/yyboo586/MQSDK"
)

var config = &mqsd.NSQConfig{
	Type:     "nsq",
	NSQDAddr: "124.220.236.38:4150",
	// 不使用NSQLookup，直接连接NSQD
	// NSQLookup: []string{},
}

var testTopics = []string{
	"test-topic-0",
	"test-topic-1",
	"test-topic-2",
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < len(testTopics); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			producer(testTopics[i])
		}(i)
	}

	var wg2 sync.WaitGroup
	for i := 0; i < len(testTopics); i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			consumer(ctx, testTopics[i])
		}(i)
	}

	wg.Wait()
	cancel()
	wg2.Wait()
}

func producer(topic string) {
	factory := mqsd.NewFactory()

	// 创建生产者
	producer, err := factory.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		message := &mqsd.Message{
			Topic: topic,
			Body:  fmt.Sprintf("%s-test %d", topic, time.Now().Unix()),
			Headers: map[string]interface{}{
				"source": "example",
			},
			Timestamp: time.Now().Unix(),
		}

		err = producer.Publish(context.Background(), topic, message)
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}

		log.Printf("publish message: %s for topic: %s", message.Body, topic)
		time.Sleep(5 * time.Second)
	}
}

func consumer(ctx context.Context, topic string) {
	factory := mqsd.NewFactory()

	consumer, err := factory.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	err = consumer.Subscribe(context.Background(), topic, "test-channel", func(msg *mqsd.Message) error {
		fmt.Printf("Received message: ID=%s, Topic=%s, Body=%s\n",
			msg.ID, msg.Topic, msg.Body)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	<-ctx.Done()
	log.Println("receive cancel signal")
}

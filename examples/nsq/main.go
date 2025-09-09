package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	mqsdk "github.com/yyboo586/MQSDK"
)

var config = &mqsdk.NSQConfig{
	Type:     "nsq",
	NSQDAddr: "124.221.243.128:4150",
	// 不使用NSQLookup，直接连接NSQD
	// NSQLookup: []string{},
}

var testTopics = []string{
	"core.push.users",
	"test-topic-1",
	"test-topic-2",
}

func main() {
	// test01()
	topic := "core.push.users"
	factory := mqsdk.NewFactory()

	// 创建生产者
	producer, err := factory.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 1000; i++ {
		body := map[string]interface{}{
			"user_ids": []string{"11111111-0000-0000-0000-000000000000", "dd58fefd-1238-4d4b-8653-705def8733dc"},
			"content": map[string]interface{}{
				fmt.Sprintf("key_%d", i): fmt.Sprintf("value_%d", i),
			},
		}

		message := &mqsdk.Message{
			Topic: topic,
			Body:  body,
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
	}
}

func test01() {
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
	factory := mqsdk.NewFactory()

	// 创建生产者
	producer, err := factory.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		message := &mqsdk.Message{
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
	factory := mqsdk.NewFactory()

	consumer, err := factory.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	err = consumer.Subscribe(context.Background(), topic, "test-channel", func(msg *mqsdk.Message) error {
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

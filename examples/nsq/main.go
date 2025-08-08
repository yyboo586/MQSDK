package main

import (
	"context"
	"fmt"
	"log"
	"time"

	mqsd "github.com/yyboo586/MQSDK"
)

func main() {
	// 创建NSQ配置
	config := &mqsd.NSQConfig{
		Type:     "nsq",
		NSQDAddr: "124.220.236.38:4150",
		// 不使用NSQLookup，直接连接NSQD
		// NSQLookup: []string{},
	}

	// 创建工厂
	factory := mqsd.NewFactory()

	// 创建生产者
	producer, err := factory.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	var body map[string]interface{} = map[string]interface{}{
		"org_id":     "1",
		"device_id":  1,
		"device_key": "1",
		"content": map[string]interface{}{
			"message": "Hello NSQ!",
			"details": map[string]interface{}{},
		},
	}

	// 发布消息
	message := &mqsd.Message{
		Topic: "core.device.alarm",
		Body:  body,
		Headers: map[string]string{
			"source": "example",
		},
		Timestamp: time.Now().Unix(),
	}

	var i int = 0
	for ; i < 1; i++ {
		err = producer.Publish(context.Background(), "core.device.alarm", message)
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}

		fmt.Println("Message published successfully")
	}

}

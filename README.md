# MQSDK - 消息队列SDK

MQSDK是一个支持NSQ、Kafka、RabbitMQ消息队列的统一Go语言SDK。

## 特性

- 支持NSQ、Kafka、RabbitMQ三种消息队列
- 统一的接口设计，易于切换不同的消息队列
- 工厂模式，简化创建和管理
- 完整的错误处理和连接管理
- 支持消息头信息
- 自动生成消息ID和时间戳

## 安装

```bash
go get github.com/yyboo586/MQSDK
```

## 快速开始

### NSQ示例

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/yyboo586/MQSDK"
)

func main() {
    // 创建NSQ配置
    config := &mqsd.NSQConfig{
        Type:      "nsq",
        NSQDAddr:  "localhost:4150",
        NSQLookup: []string{"localhost:4161"},
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
    err = consumer.Subscribe(context.Background(), "test-topic", func(msg *mqsd.Message) error {
        fmt.Printf("Received message: ID=%s, Topic=%s, Body=%s\n", 
            msg.ID, msg.Topic, string(msg.Body))
        return nil
    })
    if err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }

    // 发布消息
    message := &mqsd.Message{
        Topic: "test-topic",
        Body:  []byte("Hello NSQ!"),
        Headers: map[string]string{
            "source": "example",
        },
    }

    err = producer.Publish(context.Background(), "test-topic", message)
    if err != nil {
        log.Fatalf("Failed to publish message: %v", err)
    }

    fmt.Println("Message published successfully")
    time.Sleep(2 * time.Second)
}
```

### Kafka示例

```go
// 创建Kafka配置
config := &mqsd.KafkaConfig{
    Type:    "kafka",
    Brokers: []string{"localhost:9092"},
    GroupID: "test-group",
    Version: "2.8.0",
}
```

### RabbitMQ示例

```go
// 创建RabbitMQ配置
config := &mqsd.RabbitMQConfig{
    Type:     "rabbitmq",
    URL:      "amqp://guest:guest@localhost:5672/",
    Exchange: "test-exchange",
}
```

## 配置说明

### NSQ配置

```go
type NSQConfig struct {
    Type      string   `json:"type"`
    NSQDAddr  string   `json:"nsqd_addr"`      // NSQD地址
    NSQLookup []string `json:"nsqlookup_addrs"` // NSQLookup地址列表
}
```

### Kafka配置

```go
type KafkaConfig struct {
    Type      string   `json:"type"`
    Brokers   []string `json:"brokers"`   // Kafka broker地址列表
    GroupID   string   `json:"group_id"`  // 消费者组ID
    Version   string   `json:"version"`   // Kafka版本
}
```

### RabbitMQ配置

```go
type RabbitMQConfig struct {
    Type     string `json:"type"`
    URL      string `json:"url"`      // RabbitMQ连接URL
    Exchange string `json:"exchange"` // 交换机名称
}
```

## 消息结构

```go
type Message struct {
    ID        string            `json:"id"`        // 消息ID
    Topic     string            `json:"topic"`     // 主题
    Body      []byte            `json:"body"`      // 消息体
    Headers   map[string]string `json:"headers"`   // 消息头
    Timestamp time.Time          `json:"timestamp"` // 时间戳
}
```

## 接口说明

### Producer接口

```go
type Producer interface {
    Publish(ctx context.Context, topic string, msg *Message) error
    Close() error
}
```

### Consumer接口

```go
type Consumer interface {
    Subscribe(ctx context.Context, topic string, handler MessageHandler) error
    Unsubscribe(topic string) error
    Close() error
}
```

## 运行示例

### NSQ示例

```bash
cd examples/nsq
go run main.go
```

### Kafka示例

```bash
cd examples/kafka
go run main.go
```

### RabbitMQ示例

```bash
cd examples/rabbitmq
go run main.go
```

## 依赖

- `github.com/nsqio/go-nsq` - NSQ客户端
- `github.com/Shopify/sarama` - Kafka客户端
- `github.com/streadway/amqp` - RabbitMQ客户端
- `github.com/google/uuid` - UUID生成

## 许可证

MIT License
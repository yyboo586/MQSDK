package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	mqsdk "github.com/yourusername/mqsdk" // 根据实际路径修改
)

var (
	channel = "message_push_service_d"
)

var (
	mqHandlerOnce sync.Once
	mqHandler     *MQHandler
)

// MessageDeduplicator 消息去重器（简化版）
type MessageDeduplicator struct {
	processedMessages map[string]time.Time
	mu                sync.RWMutex
	ttl               time.Duration
}

func NewMessageDeduplicator(ttl time.Duration) *MessageDeduplicator {
	return &MessageDeduplicator{
		processedMessages: make(map[string]time.Time),
		ttl:               ttl,
	}
}

func (d *MessageDeduplicator) IsDuplicate(messageID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	
	if processedTime, exists := d.processedMessages[messageID]; exists {
		// 检查是否过期
		if time.Since(processedTime) < d.ttl {
			return true
		}
		// 已过期，删除
		delete(d.processedMessages, messageID)
	}
	return false
}

func (d *MessageDeduplicator) MarkAsProcessed(messageID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.processedMessages[messageID] = time.Now()
}

// MQHandler 消息处理器（带去重功能）
type MQHandler struct {
	consumer     mqsdk.Consumer
	deduplicator *MessageDeduplicator
}

func NewMQHandler(config *mqsdk.NSQConfig) *MQHandler {
	mqHandlerOnce.Do(func() {
		consumer, err := mqsdk.NewFactory().NewConsumer(config)
		if err != nil {
			log.Fatalf("Failed to create consumer: %v", err)
		}
		
		// 创建去重器，消息ID保留1小时
		deduplicator := NewMessageDeduplicator(1 * time.Hour)
		
		mqHandler = &MQHandler{
			consumer:     consumer,
			deduplicator: deduplicator,
		}
	})

	return mqHandler
}

func (mqHandler *MQHandler) Start(topics []string) {
	for _, topic := range topics {
		mqHandler.Register(topic, mqHandler.handleToUsers)
	}
}

func (mqHandler *MQHandler) Register(topic string, handler func(msg *mqsdk.Message) error) {
	err := mqHandler.consumer.Subscribe(context.Background(), topic, channel, handler)
	if err != nil {
		log.Printf("Failed to subscribe to topic %s: %v", topic, err)
	}
}

func (mqHandler *MQHandler) handleToUsers(msg *mqsdk.Message) error {
	// 🔥 关键：消息去重检查
	if mqHandler.deduplicator.IsDuplicate(msg.ID) {
		log.Printf("[DEDUP] Skipping duplicate message: %s", msg.ID)
		return nil // 返回 nil 表示消息已成功处理，NSQ 会确认此消息
	}
	
	// 标记为已处理
	mqHandler.deduplicator.MarkAsProcessed(msg.ID)
	
	log.Printf("Processing message: %v", msg)
	
	// 解析消息体
	userIDsInterface, ok := msg.Body.(map[string]interface{})["user_ids"]
	if !ok {
		return fmt.Errorf("body.user_ids is required")
	}

	userIDs, ok := userIDsInterface.([]interface{})
	if !ok {
		return fmt.Errorf("body.user_ids is not a []interface{}")
	}
	if len(userIDs) == 0 {
		return fmt.Errorf("user_ids is empty")
	}

	userIDsStr := make([]string, len(userIDs))
	for i, userID := range userIDs {
		userIDsStr[i], ok = userID.(string)
		if !ok {
			return fmt.Errorf("user_ids is not a []string")
		}
	}

	contentInterface, ok := msg.Body.(map[string]interface{})["content"]
	if !ok {
		return fmt.Errorf("body.content is required")
	}

	content, ok := contentInterface.(map[string]interface{})
	if !ok {
		return fmt.Errorf("body.content is not a map[string]interface{}")
	}

	contentBytes, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %v", err)
	}

	// 你的业务逻辑
	log.Printf("Saving message for users: %v, content: %s", userIDsStr, string(contentBytes))
	
	// err = mqHandler.logicsMessage.Add(context.Background(), interfaces.MessageTypeToUsers, userIDsStr, msg.ID, string(contentBytes), msg.Timestamp)
	// if err != nil {
	// 	log.Printf("[ERROR] save message error: %v", err)
	// 	// 如果保存失败，从去重列表中移除，允许重试
	// 	mqHandler.deduplicator.mu.Lock()
	// 	delete(mqHandler.deduplicator.processedMessages, msg.ID)
	// 	mqHandler.deduplicator.mu.Unlock()
	// 	return err
	// }

	// mqHandler.messagePush.NotifyByNewMessage(msg.ID)
	
	log.Printf("Successfully processed message: %s", msg.ID)
	return nil
}

func main() {
	config := &mqsdk.NSQConfig{
		Type:      "nsq",
		NSQDAddr:  "127.0.0.1:4150",
		NSQLookup: []string{},
	}

	handler := NewMQHandler(config)
	handler.Start([]string{"push_to_users"})

	// 保持程序运行
	select {}
}

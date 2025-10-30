package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MessageDeduplicator 消息去重器
type MessageDeduplicator struct {
	processedMessages map[string]time.Time
	mu                sync.RWMutex
	ttl               time.Duration
	cleanupInterval   time.Duration
	stopChan          chan struct{}
}

// NewMessageDeduplicator 创建消息去重器
// ttl: 消息ID在缓存中保留的时间
// cleanupInterval: 清理过期记录的间隔
func NewMessageDeduplicator(ttl, cleanupInterval time.Duration) *MessageDeduplicator {
	d := &MessageDeduplicator{
		processedMessages: make(map[string]time.Time),
		ttl:               ttl,
		cleanupInterval:   cleanupInterval,
		stopChan:          make(chan struct{}),
	}
	
	// 启动自动清理
	go d.startCleanup()
	
	return d
}

// IsDuplicate 检查消息是否已处理过
// 返回 true 表示是重复消息，false 表示是新消息
func (d *MessageDeduplicator) IsDuplicate(messageID string) bool {
	d.mu.RLock()
	_, exists := d.processedMessages[messageID]
	d.mu.RUnlock()
	return exists
}

// MarkAsProcessed 标记消息为已处理
func (d *MessageDeduplicator) MarkAsProcessed(messageID string) {
	d.mu.Lock()
	d.processedMessages[messageID] = time.Now()
	d.mu.Unlock()
}

// Process 处理消息（检查去重 + 标记已处理）
// 返回 true 表示应该处理此消息，false 表示是重复消息应该跳过
func (d *MessageDeduplicator) Process(messageID string, handler func() error) error {
	// 检查是否重复
	if d.IsDuplicate(messageID) {
		fmt.Printf("[DEDUP] Skipping duplicate message: %s\n", messageID)
		return nil
	}
	
	// 先标记为已处理（防止并发重复）
	d.MarkAsProcessed(messageID)
	
	// 执行实际处理逻辑
	err := handler()
	if err != nil {
		// 如果处理失败，从已处理列表中移除，允许重试
		d.mu.Lock()
		delete(d.processedMessages, messageID)
		d.mu.Unlock()
		return err
	}
	
	return nil
}

// startCleanup 启动自动清理过期记录
func (d *MessageDeduplicator) startCleanup() {
	ticker := time.NewTicker(d.cleanupInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			d.cleanup()
		case <-d.stopChan:
			return
		}
	}
}

// cleanup 清理过期的消息记录
func (d *MessageDeduplicator) cleanup() {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	now := time.Now()
	expiredCount := 0
	
	for messageID, processedTime := range d.processedMessages {
		if now.Sub(processedTime) > d.ttl {
			delete(d.processedMessages, messageID)
			expiredCount++
		}
	}
	
	if expiredCount > 0 {
		fmt.Printf("[DEDUP] Cleaned up %d expired message records\n", expiredCount)
	}
}

// Stop 停止去重器
func (d *MessageDeduplicator) Stop() {
	close(d.stopChan)
}

// GetStats 获取统计信息
func (d *MessageDeduplicator) GetStats() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()
	
	return map[string]interface{}{
		"total_tracked_messages": len(d.processedMessages),
		"ttl_seconds":            d.ttl.Seconds(),
	}
}

// 使用示例
func ExampleUsage() {
	// 创建去重器：消息ID保留1小时，每10分钟清理一次
	deduplicator := NewMessageDeduplicator(1*time.Hour, 10*time.Minute)
	defer deduplicator.Stop()
	
	// 在消息处理函数中使用
	handleMessage := func(messageID string) error {
		return deduplicator.Process(messageID, func() error {
			// 实际的业务处理逻辑
			fmt.Printf("Processing message: %s\n", messageID)
			// ... 你的业务代码 ...
			return nil
		})
	}
	
	// 模拟处理消息
	ctx := context.Background()
	_ = ctx
	
	// 第一次处理 - 成功
	handleMessage("msg-001")
	
	// 第二次处理相同消息 - 会被跳过
	handleMessage("msg-001")
	
	// 查看统计信息
	fmt.Printf("Stats: %+v\n", deduplicator.GetStats())
}

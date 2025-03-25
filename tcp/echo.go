package tcp

import (
	"bufio"
	"context"
	"redis/myredis/lib/logger"
	"redis/myredis/lib/sync/atomic"
	"redis/myredis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

/**
 * Echo 服务器实现
 * 
 * 本文件实现了一个简单的 Echo 服务器，用于测试服务器基础功能是否正常。
 * 主要功能：
 * 1. 接收客户端连接
 * 2. 将客户端发送的内容原样返回（回显）
 * 3. 支持优雅关闭，确保所有连接处理完成后再退出
 * 
 * 该实现具有以下特点：
 * - 并发安全连接
 * - 优雅关闭机制
 */

// EchoHandler 实现 Echo 服务的处理器
type EchoHandler struct {
	activeConn sync.Map   // 保存所有活跃连接，使用 sync.Map 保证线程安全，有个缺点就是高频读写会效率比较差
	closing    atomic.Boolean // 原子布尔值，标记是否正在关闭服务
}

// MakeEchoHandler 创建 EchoHandler 实例
func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// EchoClient 表示一个 Echo 客户端连接
type EchoClient struct {
	Conn    net.Conn  // 客户端网络连接
	Waiting wait.Wait // 等待机制，用于优雅关闭时等待处理中的请求完成
}

// Close 关闭客户端连接
func (c *EchoClient) Close() error {
	// 等待最多10秒，让正在处理的请求完成
	c.Waiting.WaitWithTimeout(10 * time.Second)
	// 关闭底层连接
	c.Conn.Close()
	return nil
}

// Handle 处理客户端连接
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 如果服务正在关闭，直接拒绝新连接
	if h.closing.Get() {
		_ = conn.Close()
		return
	}

	// 创建新的客户端对象并保存到活跃连接表
	client := &EchoClient{
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{})

	// 使用带缓冲的读取器
	reader := bufio.NewReader(conn)
	for {
		// 读取客户端发送的一行数据（以\n结尾）
		msg, err := reader.ReadString('\n')
		if err != nil {
			// 客户端正常关闭连接
			if err == io.EOF {
				logger.Info("连接关闭")
			} else {
				// 其他错误情况
				logger.Warn(err)
			}
			// 从活跃连接表中移除
			h.activeConn.Delete(client)
			return
		}

		// 标记开始处理请求
		client.Waiting.Add(1)
		
		// 将收到的消息原样返回给客户端
		b := []byte(msg)
		_, _ = conn.Write(b)
		
		// 标记请求处理完成
		client.Waiting.Done()
	}
}

// Close 关闭 Echo 处理器
func (h *EchoHandler) Close() error {
	logger.Info("处理器正在关闭...")
	// 设置关闭标志，拒绝新连接
	h.closing.Set(true)
	
	// 遍历所有活跃连接并逐个关闭
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Close()
		return true
	})
	return nil
}
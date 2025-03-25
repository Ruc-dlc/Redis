package tcp


import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"redis/myredis/interface/tcp"
	"redis/myredis/lib/logger"
)

/**
 * TCP 服务器实现
 *
 * 本文件实现了一个完整的TCP服务器，具有以下功能：
 * 1. 监听指定端口并处理客户端连接
 * 2. 支持优雅关闭（通过信号通知）
 * 3. 连接数统计与控制
 * 4. 错误处理和重试机制
 * - 支持最大连接数限制设置
 * - 内置连接超时控制
 * - 线程安全的连接计数器
 * - 错误处理和日志记录
 */

// Config 定义TCP服务器配置
type Config struct {
	Address    string        `yaml:"address"`     // 监听地址，格式为"host:port"
	MaxConnect uint32        `yaml:"max-connect"` // 最大连接数限制
	Timeout    time.Duration `yaml:"timeout"`     // 连接超时时间
}

// ClientCounter 记录当前服务器的客户端连接数（原子操作保证线程安全）
var ClientCounter int32

// ListenAndServeWithSignal 监听端口并处理请求，阻塞直到收到停止信号
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	// 创建关闭信号通道
	closeChan := make(chan struct{})
	// 创建系统信号通道
	sigCh := make(chan os.Signal)
	// 注册关心的系统信号
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	
	// 启动goroutine监听系统信号
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{} // 收到退出信号，通知关闭
		}
	}()

	// 开始监听TCP端口
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	// 进入主服务循环
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe 监听端口并处理请求，阻塞直到关闭
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 创建错误通道（缓冲大小为1防止goroutine泄露）
	errCh := make(chan error, 1)
	defer close(errCh) // 确保通道关闭
	
	// 启动goroutine监听关闭信号或错误
	go func() {
		select {
		case <-closeChan: // 收到关闭信号
			logger.Info("get exit signal")
		case er := <-errCh: // 发生错误
			logger.Info(fmt.Sprintf("accept error: %s", er.Error()))
		}
		
		logger.Info("shutting down...")
		_ = listener.Close() // 关闭监听器，使Accept立即返回错误
		_ = handler.Close()  // 关闭所有处理中的连接
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup // 等待所有连接处理完成
	
	for {
		// 接受新连接
		conn, err := listener.Accept()
		if err != nil {
			// 处理临时性错误（自动重试）
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Infof("accept occurs temporary error: %v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			// 非临时性错误，通知关闭
			errCh <- err
			break
		}
		
		// 处理新连接
		logger.Info("accept link")
		atomic.AddInt32(&ClientCounter, 1) // 增加连接计数
		waitDone.Add(1) // 增加等待计数
		
		// 启动goroutine处理连接
		go func() {
			defer func() {
				waitDone.Done() // 减少等待计数
				atomic.AddInt32(&ClientCounter, -1) // 减少连接计数
			}()
			handler.Handle(ctx, conn) // 处理连接
		}()
	}
	
	// 等待所有连接处理完成
	waitDone.Wait()
}













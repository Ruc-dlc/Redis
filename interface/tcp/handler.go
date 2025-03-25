package tcp

import (
	"context"
	"net"
)
// 函数签名声明，处理连接
type HandleFunc func(ctx context.Context, conn net.Conn)
//基于tcp的服务器接口定义
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
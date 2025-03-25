package redis

/**
 * Redis 连接接口定义
 *
 * 本文件定义了Redis客户端连接的核心接口，提供以下功能：
 * 1. 基础网络通信能力
 * 2. 认证与权限管理
 * 3. 发布订阅功能
 * 4. 事务状态管理
 * 5. 数据库选择和主从角色管理
 * 
 * 主要特点：
 * - 支持完整的Redis协议交互
 * - 线程安全的连接状态管理
 * - 灵活的事务处理机制
 * - 支持主从复制功能
 */

// Connection 表示与Redis客户端的连接接口
type Connection interface {
	// 基础网络操作
	Write([]byte) (int, error)  // 向客户端写入数据
	Close() error               // 关闭连接
	RemoteAddr() string         // 获取客户端地址

	// 认证与权限管理
	SetPassword(string)         // 设置连接密码
	GetPassword() string        // 获取连接密码

	// 发布订阅功能
	Subscribe(channel string)   // 订阅频道
	UnSubscribe(channel string) // 取消订阅频道
	SubsCount() int             // 获取订阅频道数量
	GetChannels() []string      // 获取所有订阅频道

	// 事务状态管理
	InMultiState() bool         // 检查是否在事务状态
	SetMultiState(bool)         // 设置事务状态
	GetQueuedCmdLine() [][][]byte // 获取排队中的命令队列
	EnqueueCmd([][]byte)        // 将命令加入队列
	ClearQueuedCmds()           // 清空命令队列
	GetWatching() map[string]uint32 // 获取WATCH监控的键
	AddTxError(err error)       // 添加事务错误
	GetTxErrors() []error       // 获取所有事务错误

	// 数据库选择
	GetDBIndex() int            // 获取当前数据库索引
	SelectDB(int)               // 选择数据库

	// 主从角色管理
	SetSlave()                  // 设置为从节点
	IsSlave() bool              // 检查是否是从节点
	SetMaster()                 // 设置为主节点
	IsMaster() bool             // 检查是否是主节点

	// 连接标识
	Name() string               // 获取连接名称
}
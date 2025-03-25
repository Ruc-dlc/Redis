package database

/**
 * 数据库引擎接口定义
 *
 * 本文件定义了Redis风格存储引擎的核心接口，包括：
 * 1. 基础数据库操作接口(DB)
 * 2. 扩展数据库引擎接口(DBEngine)
 * 3. 数据实体结构和键事件回调定义
 * 
 * 主要功能：
 * - 提供类Redis的键值存储能力
 * - 支持事务和多键操作
 * - 支持键过期和事件回调
 * - 支持RDB持久化加载
 */

import (
	"time"

	"redis/myredis/interface/redis"
	"github.com/hdt3213/rdb/core"
) 

// CmdLine 命令行的类型别名，表示Redis协议格式的命令
type CmdLine = [][]byte

// DB 基础数据库接口，提供Redis风格的存储引擎功能
type DB interface {
	// Exec 执行命令并返回结果
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
	
	// AfterClientClose 客户端关闭后的清理工作
	AfterClientClose(c redis.Connection)
	
	// Close 关闭数据库，释放资源
	Close()
	
	// LoadRDB 从RDB文件加载数据
	LoadRDB(dec *core.Decoder) error
}

// KeyEventCallback 键事件回调函数类型
// 当键被插入或删除时会被调用（可能并发调用）
type KeyEventCallback func(dbIndex int, key string, entity *DataEntity)

// DBEngine 扩展数据库引擎接口，提供更高级的功能
type DBEngine interface {
	DB // 嵌入基础DB接口
	
	// ExecWithLock 带锁执行命令（保证原子性）
	ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply
	
	// ExecMulti 执行事务（多个命令）
	ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply
	
	// GetUndoLogs 获取撤销日志（用于事务回滚）
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	
	// ForEach 遍历数据库中的所有键值对
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	
	// RWLocks 获取读写锁（用于事务）
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	
	// RWUnLocks 释放读写锁
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	
	// GetDBSize 获取数据库大小（键数量和数据大小）
	GetDBSize(dbIndex int) (int, int)
	
	// GetEntity 获取键对应的数据实体
	GetEntity(dbIndex int, key string) (*DataEntity, bool)
	
	// GetExpiration 获取键的过期时间
	GetExpiration(dbIndex int, key string) *time.Time
	
	// SetKeyInsertedCallback 设置键插入事件回调
	SetKeyInsertedCallback(cb KeyEventCallback)
	
	// SetKeyDeletedCallback 设置键删除事件回调
	SetKeyDeletedCallback(cb KeyEventCallback)
}

// DataEntity 表示存储在数据库中的数据实体
type DataEntity struct {
	Data interface{} // 实际存储的数据，可以是字符串、列表、哈希等
}
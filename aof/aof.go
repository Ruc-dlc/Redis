package aof

/**
 * AOF (Append Only File) 持久化实现
 *
 * 本文件实现了Redis风格的AOF持久化机制，主要功能包括：
 * 1. 命令追加写入AOF文件
 * 2. 支持多种fsync策略(always/everysec/no)
 * 3. AOF文件重写(压缩)
 * 4. AOF文件加载恢复数据
 * 
 * 主要特点：
 * - 高性能异步写入
 * - 线程安全设计
 * - 支持监听器回调
 * - 完善的错误处理
 */

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rdb "github.com/hdt3213/rdb/core"
	"redis/myredis/config"
	"redis/myredis/interface/database"
	"redis/myredis/lib/logger"
	"redis/myredis/lib/utils"
	"redis/myredis/redis/connection"
	"redis/myredis/redis/parser"
	"redis/myredis/redis/protocol"
)

// CmdLine 命令行的类型别名
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 20 // AOF命令队列大小(1MB)
)

// Fsync策略常量
const (
	FsyncAlways   = "always"    // 每条命令都同步写入磁盘
	FsyncEverySec = "everysec"  // 每秒同步一次
	FsyncNo       = "no"        // 由操作系统决定同步时机
)

// payload 表示要写入AOF的命令负载
type payload struct {
	cmdLine CmdLine    // Redis命令
	dbIndex int        // 数据库索引
	wg      *sync.WaitGroup // 等待组(用于同步)
}

// Listener AOF监听器接口
type Listener interface {
	Callback([]CmdLine) // 命令回调
}

// Persister AOF持久化器
type Persister struct {
	ctx         context.Context
	cancel      context.CancelFunc
	db          database.DBEngine     // 数据库引擎
	tmpDBMaker  func() database.DBEngine // 临时数据库创建函数
	aofChan     chan *payload         // AOF命令通道
	aofFile     *os.File              // AOF文件句柄
	aofFilename string                // AOF文件名
	aofFsync    string                // Fsync策略
	aofFinished chan struct{}         // AOF完成信号
	pausingAof  sync.Mutex            // AOF暂停锁
	currentDB   int                   // 当前数据库索引
	listeners   map[Listener]struct{} // 监听器集合
	buffer      []CmdLine             // 命令缓冲区
}

// NewPersister 创建新的AOF持久化器
func NewPersister(db database.DBEngine, filename string, load bool, fsync string, 
	tmpDBMaker func() database.DBEngine) (*Persister, error) {
	
	persister := &Persister{
		aofFilename: filename,
		aofFsync:    strings.ToLower(fsync),
		db:          db,
		tmpDBMaker:  tmpDBMaker,
		currentDB:   0,
	}
	
	// 如果需要，加载AOF文件
	if load {
		persister.LoadAof(0)
	}
	
	// 打开AOF文件
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	
	// 初始化通道和监听器
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	
	// 启动命令监听协程
	go func() {
		persister.listenCmd()
	}()
	
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	
	// 如果配置为每秒同步，启动同步协程
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	
	return persister, nil
}

// RemoveListener 移除监听器
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// SaveCmdLine 保存命令到AOF
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	if persister.aofChan == nil {
		return
	}

	// always策略直接写入
	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}

	// 其他策略放入通道异步处理
	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
}

// listenCmd 监听命令通道并写入AOF
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

// writeAof 实际写入AOF文件
func (persister *Persister) writeAof(p *payload) {
	persister.buffer = persister.buffer[:0] // 清空缓冲区
	persister.pausingAof.Lock()            // 获取锁
	defer persister.pausingAof.Unlock()
	
	// 如果数据库切换，先写入SELECT命令
	if p.dbIndex != persister.currentDB {
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return
		}
		persister.currentDB = p.dbIndex
	}
	
	// 写入命令数据
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	
	// 通知监听器
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	
	// 如果是always策略，立即同步
	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

// LoadAof 从AOF文件加载数据
func (persister *Persister) LoadAof(maxBytes int) {
	// 临时禁用aofChan
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	// 打开AOF文件
	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	// 先尝试加载RDB格式
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		file.Seek(0, io.SeekStart)
	} else {
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		maxBytes = maxBytes - decoder.GetReadCount()
	}

	// 解析AOF命令
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	fakeConn := connection.NewFakeConn()
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		ret := persister.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}
		if strings.ToLower(string(r.Args[0])) == "select" {
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// Fsync 同步AOF文件到磁盘
func (persister *Persister) Fsync() {
	persister.pausingAof.Lock()
	if err := persister.aofFile.Sync(); err != nil {
		logger.Errorf("fsync failed: %v", err)
	}
	persister.pausingAof.Unlock()
}

// Close 关闭AOF持久化器
func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished // 等待处理完成
		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	persister.cancel()
}

// fsyncEverySecond 每秒同步一次
func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.Fsync()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

// generateAof 生成新的AOF文件(重写)
func (persister *Persister) generateAof(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile
	tmpAof := persister.newRewriteHandler()
	tmpAof.LoadAof(int(ctx.fileSize))

	// 为每个数据库生成SELECT命令
	for i := 0; i < config.Properties.Databases; i++ {
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		
		// 遍历数据库中的所有键值对
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			// 写入键值对命令
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			// 写入过期时间命令
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}
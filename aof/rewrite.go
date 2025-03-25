package aof

import (
	"io"
	"os"
	"strconv"

	"redis/myredis/config"
	"redis/myredis/lib/logger"
	"redis/myredis/lib/utils"
	"redis/myredis/redis/protocol"
)

/*
*AOF重写流程：完整的重写过程包括准备、执行和完成三个阶段
*混合持久化支持：可选择生成RDB格式前导或纯AOF格式
*线程安全：通过锁机制确保重写期间数据一致性
*临时文件处理：使用临时文件保证操作的原子性
*命令追加：正确处理重写期间的新命令
*/

// newRewriteHandler 创建一个新的AOF处理器用于重写过程
func (persister *Persister) newRewriteHandler() *Persister {
	h := &Persister{}
	h.aofFilename = persister.aofFilename
	h.db = persister.tmpDBMaker() // 使用临时数据库避免影响主数据库
	return h
}

// RewriteCtx 保存AOF重写过程的上下文信息
type RewriteCtx struct {
	tmpFile  *os.File // 临时文件句柄
	fileSize int64    // 原始AOF文件大小
	dbIdx    int      // 开始重写时选择的数据库索引
}

// Rewrite 执行完整的AOF重写流程
func (persister *Persister) Rewrite() error {
	// 1. 准备重写上下文
	ctx, err := persister.StartRewrite()
	if err != nil {
		return err
	}

	// 2. 执行实际重写操作
	err = persister.DoRewrite(ctx)
	if err != nil {
		return err
	}

	// 3. 完成重写并清理资源
	persister.FinishRewrite(ctx)
	return nil
}

// DoRewrite 实际执行AOF重写操作
// 注意：通常应该使用Rewrite方法，此方法公开仅用于测试
func (persister *Persister) DoRewrite(ctx *RewriteCtx) (err error) {
	// 根据配置选择重写格式
	if !config.Properties.AofUseRdbPreamble {
		logger.Info("生成AOF格式前导")
		err = persister.generateAof(ctx)
	} else {
		logger.Info("生成RDB格式前导")
		err = persister.generateRDB(ctx)
	}
	return err
}

// StartRewrite 准备重写过程
func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// 加锁暂停AOF写入，确保数据一致性
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// 确保所有缓冲数据写入磁盘
	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("文件同步失败")
		return nil, err
	}

	// 获取当前AOF文件大小
	fileInfo, _ := os.Stat(persister.aofFilename)
	filesize := fileInfo.Size()

	// 创建临时文件用于重写
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("创建临时文件失败")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		dbIdx:    persister.currentDB, // 保存当前数据库索引
	}, nil
}

// FinishRewrite 完成重写过程
func (persister *Persister) FinishRewrite(ctx *RewriteCtx) {
	// 加锁确保原子性操作
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	tmpFile := ctx.tmpFile

	// 处理重写期间执行的命令
	errOccurs := func() bool {
		/* 读取重写期间执行的写命令 */
		src, err := os.Open(persister.aofFilename)
		if err != nil {
			logger.Error("打开AOF文件失败: " + err.Error())
			return true
		}
		defer func() {
			_ = src.Close()
			_ = tmpFile.Close()
		}()

		// 定位到重写开始时的文件位置
		_, err = src.Seek(ctx.fileSize, 0)
		if err != nil {
			logger.Error("文件定位失败: " + err.Error())
			return true
		}
		
		// 同步临时文件的数据库索引
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
		_, err = tmpFile.Write(data)
		if err != nil {
			logger.Error("临时文件写入失败: " + err.Error())
			return true
		}
		
		// 将重写期间的新命令追加到临时文件
		_, err = io.Copy(tmpFile, src)
		if err != nil {
			logger.Error("AOF文件复制失败: " + err.Error())
			return true
		}
		return false
	}()
	if errOccurs {
		return
	}

	// 原子性替换当前AOF文件
	_ = persister.aofFile.Close()
	if err := os.Rename(tmpFile.Name(), persister.aofFilename); err != nil {
		logger.Warn(err)
	}
	
	// 重新打开AOF文件用于后续写入
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	persister.aofFile = aofFile

	// 重新写入SELECT命令确保数据库状态一致
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
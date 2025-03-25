package aof

import (
	"os"
	"strconv"
	"time"

	"redis/myredis/config"
	"redis/myredis/datastruct/dict"
	List "redis/myredis/datastruct/list"
	"redis/myredis/datastruct/set"
	SortedSet "redis/myredis/datastruct/sortedset"
	"redis/myredis/interface/database"
	"redis/myredis/lib/logger"
	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
)

/**
 * AOF 转 RDB 功能实现
 * 
 * 本文件实现了将AOF文件内容转换为RDB格式的功能，
 * 主要用于数据持久化和主从复制场景。
 * 
 * 主要特点：
 * - 支持同步和异步两种生成方式
 * - 完整的RDB文件格式支持
 * - 线程安全的设计
 * - 支持所有Redis数据结构
 */

// GenerateRDB 同步生成RDB文件
func (persister *Persister) GenerateRDB(rdbFilename string) error {
	ctx, err := persister.startGenerateRDB(nil, nil)
	if err != nil {
		return err
	}
	err = persister.generateRDB(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	// 原子性重命名临时文件
	return os.Rename(ctx.tmpFile.Name(), rdbFilename)
}

// GenerateRDBForReplication 为复制生成RDB文件(异步)
// listener: 接收后续更新的监听器
// hook: 在AOF暂停期间执行的回调函数
func (persister *Persister) GenerateRDBForReplication(rdbFilename string, listener Listener, hook func()) error {
	ctx, err := persister.startGenerateRDB(listener, hook)
	if err != nil {
		return err
	}

	err = persister.generateRDB(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	// 原子性重命名临时文件
	return os.Rename(ctx.tmpFile.Name(), rdbFilename)
}

// startGenerateRDB 开始生成RDB的准备工作
func (persister *Persister) startGenerateRDB(newListener Listener, hook func()) (*RewriteCtx, error) {
	// 加锁确保AOF暂停期间不会有写入
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// 确保所有数据写入磁盘
	if err := persister.aofFile.Sync(); err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// 获取当前AOF文件大小
	fileInfo, _ := os.Stat(persister.aofFilename)
	filesize := fileInfo.Size()

	// 创建临时文件
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}

	// 添加监听器(如果存在)
	if newListener != nil {
		persister.listeners[newListener] = struct{}{}
	}

	// 执行回调函数(如果存在)
	if hook != nil {
		hook()
	}

	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
	}, nil
}

// generateRDB 实际执行AOF到RDB的转换
func (persister *Persister) generateRDB(ctx *RewriteCtx) error {
	// 创建临时处理器加载AOF数据
	tmpHandler := persister.newRewriteHandler()
	tmpHandler.LoadAof(int(ctx.fileSize))

	// 初始化RDB编码器
	encoder := rdb.NewEncoder(ctx.tmpFile).EnableCompress()
	if err := encoder.WriteHeader(); err != nil {
		return err
	}

	// 设置RDB文件头信息
	auxMap := map[string]string{
		"redis-ver":    "6.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}

	// 根据配置设置aof-preamble标志
	if config.Properties.AofUseRdbPreamble {
		auxMap["aof-preamble"] = "1"
	}

	// 写入辅助字段
	for k, v := range auxMap {
		if err := encoder.WriteAux(k, v); err != nil {
			return err
		}
	}

	// 遍历所有数据库
	for i := 0; i < config.Properties.Databases; i++ {
		keyCount, ttlCount := tmpHandler.db.GetDBSize(i)
		if keyCount == 0 {
			continue // 跳过空数据库
		}

		// 写入数据库头
		if err := encoder.WriteDBHeader(uint(i), uint64(keyCount), uint64(ttlCount)); err != nil {
			return err
		}

		// 遍历数据库中的键值对
		var err error
		tmpHandler.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			var opts []interface{}
			if expiration != nil {
				opts = append(opts, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}

			// 根据数据类型选择不同的序列化方式
			switch obj := entity.Data.(type) {
			case []byte: // 字符串类型
				err = encoder.WriteStringObject(key, obj, opts...)
			case List.List: // 列表类型
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(i int, v interface{}) bool {
					bytes, _ := v.([]byte)
					vals = append(vals, bytes)
					return true
				})
				err = encoder.WriteListObject(key, vals, opts...)
			case *set.Set: // 集合类型
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(m string) bool {
					vals = append(vals, []byte(m))
					return true
				})
				err = encoder.WriteSetObject(key, vals, opts...)
			case dict.Dict: // 哈希类型
				hash := make(map[string][]byte)
				obj.ForEach(func(key string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hash[key] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case *SortedSet.SortedSet: // 有序集合类型
				var entries []*model.ZSetEntry
				obj.ForEachByRank(int64(0), obj.Len(), true, func(element *SortedSet.Element) bool {
					entries = append(entries, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, entries, opts...)
			}

			// 如果出现错误则终止遍历
			return err == nil
		})

		if err != nil {
			return err
		}
	}

	// 写入结束标记
	return encoder.WriteEnd()
}
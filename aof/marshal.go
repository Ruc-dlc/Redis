package aof

/**
 * AOF 命令序列化工具
 * 
 * 本文件实现了将内存数据结构序列化为Redis协议格式的命令，
 * 主要用于AOF持久化和AOF重写功能。
 * 
 * 支持的数据结构包括：
 * - 字符串(String)
 * - 列表(List)
 * - 集合(Set)
 * - 哈希(Hash)
 * - 有序集合(SortedSet)
 * - 键过期时间(Expire)
 */

import (
	"strconv"
	"time"

	"redis/myredis/datastruct/dict"
	List "redis/myredis/datastruct/list"
	"redis/myredis/datastruct/set"
	SortedSet "redis/myredis/datastruct/sortedset"
	"redis/myredis/interface/database"
	"redis/myredis/redis/protocol"
)

// 预定义Redis命令字节表示
var (
	setCmd        = []byte("SET")
	rPushAllCmd   = []byte("RPUSH")
	sAddCmd       = []byte("SADD")
	hMSetCmd      = []byte("HMSET")
	zAddCmd       = []byte("ZADD")
	pExpireAtBytes = []byte("PEXPIREAT")
)

// EntityToCmd 将数据实体序列化为Redis命令
func EntityToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}
	
	// 根据数据类型选择不同的序列化方法
	switch val := entity.Data.(type) {
	case []byte:
		return stringToCmd(key, val)      // 字符串类型
	case List.List:
		return listToCmd(key, val)       // 列表类型
	case *set.Set:
		return setToCmd(key, val)        // 集合类型
	case dict.Dict:
		return hashToCmd(key, val)       // 哈希类型
	case *SortedSet.SortedSet:
		return zSetToCmd(key, val)       // 有序集合类型
	}
	return nil
}

// stringToCmd 字符串类型序列化为SET命令
func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd     // "SET"
	args[1] = []byte(key) // 键名
	args[2] = bytes      // 值
	return protocol.MakeMultiBulkReply(args)
}

// listToCmd 列表类型序列化为RPUSH命令
func listToCmd(key string, list List.List) *protocol.MultiBulkReply {
	args := make([][]byte, 2+list.Len())
	args[0] = rPushAllCmd  // "RPUSH"
	args[1] = []byte(key)  // 键名
	
	// 遍历列表元素
	list.ForEach(func(i int, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i] = bytes  // 列表元素
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

// setToCmd 集合类型序列化为SADD命令
func setToCmd(key string, set *set.Set) *protocol.MultiBulkReply {
	args := make([][]byte, 2+set.Len())
	args[0] = sAddCmd     // "SADD"
	args[1] = []byte(key) // 键名
	
	// 遍历集合元素
	i := 0
	set.ForEach(func(val string) bool {
		args[2+i] = []byte(val) // 集合元素
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

// hashToCmd 哈希类型序列化为HMSET命令
func hashToCmd(key string, hash dict.Dict) *protocol.MultiBulkReply {
	args := make([][]byte, 2+hash.Len()*2)
	args[0] = hMSetCmd    // "HMSET"
	args[1] = []byte(key) // 键名
	
	// 遍历哈希字段
	i := 0
	hash.ForEach(func(field string, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i*2] = []byte(field) // 字段名
		args[3+i*2] = bytes         // 字段值
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

// zSetToCmd 有序集合类型序列化为ZADD命令
func zSetToCmd(key string, zset *SortedSet.SortedSet) *protocol.MultiBulkReply {
	args := make([][]byte, 2+zset.Len()*2)
	args[0] = zAddCmd     // "ZADD"
	args[1] = []byte(key) // 键名
	
	// 按排名遍历有序集合元素
	i := 0
	zset.ForEachByRank(int64(0), int64(zset.Len()), true, func(element *SortedSet.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64) // 分数转为字符串
		args[2+i*2] = []byte(value)       // 分数
		args[3+i*2] = []byte(element.Member) // 成员
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

// MakeExpireCmd 创建键过期命令(PEXPIREAT)
func MakeExpireCmd(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes                       // "PEXPIREAT"
	args[1] = []byte(key)                          // 键名
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10)) // 毫秒级时间戳
	return protocol.MakeMultiBulkReply(args)
}
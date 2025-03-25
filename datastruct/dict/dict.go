package dict

//并发哈希表，提供给其他包访问的接口说明

//消费者函数，返回true表示继续消费，返回false表示停止消费，用于遍历函数中
type Consumer func(key string, val interface{}) bool

//k-v存储结构字典接口
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (val interface{}, result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
	DictScan(cursor int, count int, pattern string) ([][]byte, int)
}
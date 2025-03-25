package dict

import (
	"math"
	"math/rand"
	"redis/myredis/lib/wildcard"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
//以分片形式附带读写锁控制并发安全，同时以切片形式组织多个分片，元信息包含分片数目和总元素个数作为哈希表
//同时，必须注意的是，为了快速拿到哈希表的索引，将哈希表的长度设置为2的幂次方，允许调用方传入期望的哈希表长度
//为了保障并发安全，需要在有竞争的条件下，加锁操作，需要加锁的场景包括：修改元素个数，分片的访问等

//定义分片结构体，每个分片包含哈希表本身与同步包里面的读写锁用来控制并发读写
type shard struct{
	m map[string]interface{}
	mutex sync.RWMutex
}
// 支持并发安全的映射表，提供增删改查并发安全版本的接口
type ConcurrentDict struct{
	table []*shard
	count int32 //记录总元素个数
	shardCount int //记录整个并发哈希表含有的分片个数
}
//传入参数，拿到大于等于输入的最小2的幂次方数，后续方便计算哈希值对应的分片索引
//为什么方便后续的计算，是因为可以将取模运算变为逐位与操作，效率很高
func computeCapacity(param int) int{
	//分片数目太小就直接返回16作为最小哈希表长度，避免后续频繁扩容
	if param<=16{
		return 16
	}
	//将输入的参数-1，然后不断右移逐位或，直到最高位1以下全部数位均为1，
	n:=param-1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	//溢出变成负数的时候，取最大
	if n < 0 {
		return math.MaxInt32
	}
	//+1拿到大于等于输入的最小2的幂次方数
	return n + 1
}
//传入一个期望的分片容量，构造函数创建一个支持并发的哈希表
func MakeConcurrent(shardCount int) *ConcurrentDict{
	//期望大小为1直接创建，无需计算分片数目
	if shardCount == 1 {
		table := []*shard{
			{
				m: make(map[string]interface{}),
			},
		}
		return &ConcurrentDict{
			count:      0,
			table:      table,
			shardCount: shardCount,
		}
	}
	//传入期望的分片数目，计算出方便后续哈希计算的真实分片数目，大于等于的最小2的幂次方
	shardCount = computeCapacity(shardCount)
	//创建一个分片切片，每个分片包含一个哈希表以及一个读写锁控制并发安全
	table := make([]*shard, shardCount)
	//对每个分片进行初始化
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
	return d
}
//质数基数
const prime32 = uint32(16777619)
//fnv-1a哈希算法，用于计算key对应哈希值,最终返回一个32位的无符号哈希值
func fnv32(key string) uint32 {
	//偏移量基数
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		//逐字节的处理传入key，哈希值乘质数基数，然后与当前字节异或，直到所有字节处理完毕
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
//传入key，然后拿到对应的哈希表分片索引
func (dict *ConcurrentDict) spread(key string) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	//如果整个表长度为1不需要计算哈希值直接返回0，放第一个位置
	if len(dict.table) == 1 {
		return 0
	}
	//分别拿key对应的哈希值与哈希表长
	hashCode := fnv32(key)
	tableSize := uint32(len(dict.table))
	//由于表长一定为为2的幂次方，自减1以后，低位全是1，直接与哈希值逐位与，拿到取余后的值，效率比直接取模快很多
	return (tableSize - 1) & hashCode
}
//传入索引，获取对应分片结构体
func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}
//传入key，获取对应的value值
func (dict *ConcurrentDict) Get(key string) (val interface{},exists bool){
	if dict==nil{
		panic("dict is nil")
	}
	//通过哈希函数，拿到key所在的分片下标
	//spread函数内部会先拿到哈希值，然后与表长-1逐位与，拿到key应放置的分片下标
	index := dict.spread(key)
	s := dict.getShard(index)
	//读数据时，先上锁
	s.mutex.Lock()
	//回收函数调用栈以前，释放锁
	defer s.mutex.Unlock()
	//拿到对应的值与是否存在，返回
	val, exists = s.m[key]
	return
}
//外部已经加锁的情形下，获取对应的value值
func (dict *ConcurrentDict) GetWithLock(key string) (val interface{},exists bool){
	if dict==nil{
		panic("dict is nil")
	}
	//通过哈希函数，拿到key所在的index下标
	index := dict.spread(key)
	s:=dict.getShard(index)
	val,exists=s.m[key]
	return
}
//拿哈希表的元素个数,同样需要支持并发安全
func (dict *ConcurrentDict) Len() int{
	if dict==nil{
		panic("dict is nil")
	}
	//利用sync/atomic包下的原子操作保障读取并发安全，底层硬件利用cas原理实现
	return int(atomic.LoadInt32(&dict.count))
}
//插入kv，支持并发安全
func (dict *ConcurrentDict) Put(key string,val interface{}) int{
	if dict == nil {
		panic("dict is nil")
	}
	//拿到key应该存放的分片下标
	index := dict.spread(key)
	s := dict.getShard(index)
	//插入以前需要先上锁
	s.mutex.Lock()
	defer s.mutex.Unlock()
	//如果键值对已经存在，直接覆盖旧值，返回0，表示没有新增键值对
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	//不存在，则插入键值对，同时返回1表示新增1个元素
	s.m[key] = val
	dict.addCount()
	return 1
}
//在持有锁的情况下，插入一个键值对
func (dict *ConcurrentDict) PutWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}
//当键值对不存在时，才插入键值对
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()
    //当键值对已经存在了，那么直接返回0，不会插入
	if _, ok := s.m[key]; ok {
		return 0
	}
	//不存在，则插入键值对，同时返回1表示新增1个元素
	s.m[key] = val
	dict.addCount()
	return 1
}
//持有锁的情况下，去插入键值对，当然了是在键不存在时，插入，不会对旧值进行覆盖
func (dict *ConcurrentDict) PutIfAbsentWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}
//插入键值对，覆盖旧值，并发安全
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	//如果键值对存在了，那么覆盖旧值，返回1
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	//不存在也不进行插入，返回0，表示未修改键值对
	return 0
}
//持有锁的情况下，对旧值进行覆盖
func (dict *ConcurrentDict) PutIfExistsWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}
//移除键值对，并发安全
func (dict *ConcurrentDict) Remove(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	//键值对存在的时候，返回删除的值与更改数量，同时对总元素减1
	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	//不存在返回空和0，表示未修改键值对
	return nil, 0
}
//持有锁的情形，移除键值对
func (dict *ConcurrentDict) RemoveWithLock(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return val, 0
}
//并发安全，需要传入参数地址，来添加与减少哈希表的总元素个数，利用原子操作对有符号32位加减一
func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}
func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}
//遍历函数，允许调用方，传入回调函数逻辑，定义对元素进行操作，当返回false时，遍历函数停止遍历
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}
	//range遍历整个哈希并发哈希表,返回的是每个分片
	for _, s := range dict.table {
		//访问分片以前上读锁
		s.mutex.RLock()
		//闭包函数，定义遍历分片的实体
		f := func() bool {
			//在f函数结束以前会释放读锁
			defer s.mutex.RUnlock()
			//遍历分片内所有键值对
			for key, value := range s.m {
				//回调函数对遍历到的键值对进行操作，同时返回是否继续遍历下一个元素
				continues := consumer(key, value)
				if !continues {
					return false
				}
			}
			return true
		}
		//consumer这边返回false以后，f也会返回false，于是就终止遍历下一个分片
		if !f() {
			break
		}
	}
}
//返回当下哈希表包含的所有键
func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	//传入自定义consumer，需要写入终止逻辑
	dict.ForEach(func(key string, val interface{}) bool {
		//当未遍历玩整个哈希表时，继续追加键到keys数组中
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			//foreach函数虽然添加了读锁，但是这块还是防御性逻辑，防止超出切片长度越界
			keys = append(keys, key)
		}
		//由于是取哈希表内的所有元素，所以始终返回true，继续遍历下一个元素
		return true
	})
	return keys
}
//传入目标分片，随机返回该分片哈希表内的一个键
func (shard *shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	//go的特性，在遍历map时，会随机选取一个开始位置，最终的结果就是会拿到不同的键值对
	for key := range shard.m {
		return key
	}
	//如果该分片的哈希表没有键值对，那么返回空字符串
	return ""
}
//随机返回哈希表内的指定数量的键,但是需要注意的是，可能会包含重复的键
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	//如果期望拿到的数字比哈希表的全部元素都要多，那么直接返回全部键
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	//传入当前纳秒时间戳作为随机数种子生成随机数生成器
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	//逻辑是每次随机在一个分片上，随机取一个键值对，那么可能会拿到相同的键
	for i := 0; i < limit; {
		//随机获取一个分片
		s := dict.getShard(uint32(nR.Intn(shardCount)))
		if s == nil {
			continue
		}
		//在该分片上随机拿一个key
		key := s.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}
//随机返回哈希表内的指定数量的键，对返回的键去重
func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	//这块定义一个值为空结构体的map，是为了利用哈希表查找o1的时间复杂度去过滤掉重复的键
	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(nR.Intn(shardCount))
		s := dict.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			//随机一个分片拿到的一个随机的键，需判断是否已经存在,每次把不存在的键都收起来
			if _, exists := result[key]; !exists {
				//distinct逻辑，把不重复的键收集起来
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	//只需要取键即可，value为空结构体
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}
//清除整个哈希表，直接调用构造函数，构造一个等长的哈希表
func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}
//传入一系列的键，拿到这些键出现的分片索引，并排序，reverse可以指定从大到小，还是从小到大，默认从大到小
//该接口的存在的意义是为了，多个线程尝试给多个分片进行加锁时，应当保证固定的顺序添加锁，防止发生死锁
func (dict *ConcurrentDict) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		//拿到键对应的分片索引
		index := dict.spread(key)
		indexMap[index] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	//将keys对应的分片索引排序，方便后续加锁，避免死锁
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	//返回排序后的分片索引
	return indices
}
//对写和读操作进行加锁，传入写操作的键和读操作的键，按照固定的顺序进行加锁，避免死锁
func (dict *ConcurrentDict) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	//拿到此次需要操作的读与写的键的分片下标，按照从小到大升序排列
	indices := dict.toLockIndices(keys, false)
	//对要写的键所对应的分片，过滤去重一下，拿到此次所有要操作的分片下标，空结构体不占用内存
	writeIndexSet := make(map[uint32]struct{})
	//遍历此次所有的需要写操作的键
	for _, wKey := range writeKeys {
		idx := dict.spread(wKey)
		writeIndexSet[idx] = struct{}{}
	}
	//多线程按照固定顺序进行加锁，遍历此次需要操作的所有分片
	for _, index := range indices {
		//先确认该分片是否是写操作
		_, w := writeIndexSet[index]
		//拿到这个分片的锁
		mu := &dict.table[index].mutex
		//如果这个键是需要加写操作，那么加写锁
		if w {
			mu.Lock()
		} else {
			//否则读锁即可
			mu.RLock()
		}
	}
}
//对写和读操作进行解锁，传入写操作的键和读操作的键，按照固定的顺序进行解锁，避免死锁
func (dict *ConcurrentDict) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	//解锁时的固定顺序，应当与当初的加锁顺序相反才对，从大到小进行降序排序，拿到此次涉及到的所有分片下标
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(wKey)
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		//当前分片是进行写操作
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			//加了写锁，相应的解开写锁
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}
//将字符串切片，转换为字节切片，每个字符串的每个字符为一个字节，一个字符串转换为一个一维字节切片，每个元素对应一个字符的ascii码
func stringsToBytes(strSlice []string) [][]byte {
	byteSlice := make([][]byte, len(strSlice))
	for i, str := range strSlice {
		byteSlice[i] = []byte(str)
	}
	return byteSlice
}
//scan命令实际原生是可以指定键的类型的，此次只针对hash结构进行扫描
//增量扫描哈希表，通常搭配循环使用，状态码拿到0表示全部遍历完成，返回值并不固定为count数量，而是尽量接近预期值，传入游标位置，模式，与期望匹配键的数量，去找哈希表内匹配成功的键,思想是分批次进行扫描，避免一次性扫描很大的哈希表造成性能问题
//返回字节切片形式的匹配结果，与状态码或者下一次扫描的起始分片索引，返回0表示成功，返回-1表示模式编译失败，正整数为下次起始的分片索引
func (dict *ConcurrentDict) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	size := dict.Len()
	result := make([][]byte, 0)
	//如果模式是模糊全部匹配，直接返回整个哈希表所有的键
	if pattern == "*" && count >= size {
		return stringsToBytes(dict.Keys()), 0
	}
	//首先将模式编译为正则表达式
	matchKey, err := wildcard.CompilePattern(pattern)
	//模式编译失败，直接返回-1表示失败状态
	if err != nil {
		return result, -1
	}	
	//拿到哈希表的长度和初始扫描的分片下标索引
	shardCount := len(dict.table)
	shardIndex := cursor
	//逐个分片进行扫描，直到找到期望数量的匹配键，或者遍历完所有分片
	for shardIndex < shardCount {
		//拿到每次操作的分片
		shard := dict.table[shardIndex]
		shard.mutex.RLock()
		//提前预估即将扫描的键的数目是否会超出期望值并且要求至少遍历完一个分片，才退出循环，返回匹配到的键与下一次扫描的起始分片索引，防止一次性扫描过多分片造成性能问题
		if len(result)+len(shard.m) > count && shardIndex > cursor {
			shard.mutex.RUnlock()
			//分批次返回扫描到的键
			return result, shardIndex
		}
		//在当前的分片哈希表中去扫描检查与模式是否匹配的键，加入到结果切片中
		for key := range shard.m {
			//模式匹配成功
			if pattern == "*" || matchKey.IsMatch(key) {
				result = append(result, []byte(key))
			}
		}
		//解开当前分片的读锁
		shard.mutex.RUnlock()
		//更新分片索引，表示查找下一个分片的哈希表
		shardIndex++
	}
	//返回匹配到的键切片与0.0表示整个redis表已全部扫描完毕
	return result, 0
}
package lock

import (
	"sort"
	"sync"
)

//该文件用于控制并发，对哈希表进行上锁，针对此次要操作的分片进行上锁，同时支持对多个key进行上锁
//通过控制上锁与解锁的顺序，来避免发生死锁的情况，细化了锁的粒度到分片上而不是直接锁住整个哈希表，提高并发度
//值得一提的是，对操作的key的类型还提供读或写选择，来决定对相应的分片上读锁还是写锁，进一步一定程度上提高并发度


//给定keys合集，映射到对应的分片索引，管理分片合集上的读写锁控制并发安全
type Locks struct{
	table []*sync.RWMutex
}
//传入tableSize，返回一个Locks实例
func Make(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{
		table: table,
	}
}
//质数基数
const (
	prime32=uint32(16777619)
)
//fnv32哈希算法，传入key，返回32位哈希值
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
//传入key的哈希值，拿到放置的分片索引
func (locks *Locks) spread(hashCode uint32) uint32{
	if locks==nil{
		panic("dict is nil")
	}
	tableSize:=uint32(len(locks.table))
	//由于保证了tableSize必然为2的幂次方，直接采用效率更高的位运算代替取模运算来截取尾部的k位
	return hashCode&(tableSize-1)
}
//给定单个key，给目标分片上写锁
func (locks *Locks) Lock(key string) {
	//先拿哈希值，再拿索引
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}
//给定单个key，给目标分片上读锁
func (locks *Locks) RLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RLock()
}
//给单个解写锁
func (locks *Locks) UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}
//给单个解读锁
func (locks *Locks) RUnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RUnlock()
}
//对keys合集涉及到的分片合集，排序，后续操作按照固定顺序上锁，从而避免死锁,reverse控制降序还是升序
func (locks *Locks) toLockIndices(keys []string,reverse bool) []uint32{
	//先拿到涉及到的distinct分片index合集,值用空结构体不占内存
	indexMap:=make(map[uint32]struct{})
	for _,key:=range keys{
		//遍历传入的keys合集，拿到每个key对应的分片索引
		index:=locks.spread(fnv32(key))
		indexMap[index]=struct{}{}
	}
	//拿到所有分片索引以后，开始排序确定上锁顺序
	indices:=make([]uint32,len(indexMap))
	for index,_:=range indexMap{
		indices=append(indices,index)
	}
	//调用切片排序接口
	sort.Slice(indices,func(i,j int)bool{
		//reverse为false时，升序排列，为true时，降序排列
		if !reverse{
			return indices[i]<indices[j]
		}
		return indices[i]>indices[j]
	})
	return indices
}
//传入此时操作的keys合集，给涉及到的分片按照固定顺序上写锁
func (lock *Locks)Locks(keys ...string){
	//先拿到涉及到的分片索引合集，按照升序排列
	indices:=lock.toLockIndices(keys,false)
	//遍历分片按照固定顺序上锁
	for _,index:=range indices{
		lock.table[index].Lock()
	}
}
//传入此时操作的keys合集，给涉及到的分片按照固定顺序上读锁
func (lock *Locks)RLocks(keys ...string){
	//先拿到涉及到的分片索引合集，按照升序排列
	indices:=lock.toLockIndices(keys,false)
	//遍历分片按照固定顺序上锁
	for _,index:=range indices{
		lock.table[index].RLock()
	}
}
//解锁
func (lock *Locks)Unlocks(keys ...string){
	//先拿到涉及到的分片索引合集，按照降序排列
	indices:=lock.toLockIndices(keys,true)
	//遍历分片按照固定顺序(之前上锁的逆序)解锁
	for _,index:=range indices{
		lock.table[index].Unlock()
	}
}
func (lock *Locks)RUnlocks(keys ...string){
	indices:=lock.toLockIndices(keys,true)
	for _,index:=range indices{
		lock.table[index].RUnlock()
	}
}
//传入需要读写的keys合集，给涉及到的分片按照固定顺序上读或者写锁
func (lock *Locks)RWLocks(writeKeys []string,readKeys []string){
	keys:=append(writeKeys, readKeys...)
	//拿加锁顺序
	indices:=lock.toLockIndices(keys,false)
	//去重,收集到要写的分片
	writeIndexSet:=make(map[uint32]struct{})
	for _,index:=range writeKeys{
		index:=lock.spread(fnv32(index))
		writeIndexSet[index]=struct{}{}
	}
	//有序对分片上读或者写锁
	for _,index:=range indices{
		//确定该分片是否是上写锁
		_,exists:=writeIndexSet[index]
		//拿到该分片的锁实例
		mu:=lock.table[index]
		if exists{
			mu.Lock()
		}else{
			mu.RLock()
		}
	}
}
func (lock *Locks)RWUnLock(writeKeys []string,readKeys []string){	
	keys:=append(writeKeys, readKeys...)
	//拿解锁顺序
	indices:=lock.toLockIndices(keys,true)
	//去重,收集到写分片
	writeIndexSet:=make(map[uint32]struct{})
	for _,index:=range writeKeys{
		index:=lock.spread(fnv32(index))
		writeIndexSet[index]=struct{}{}
	}
	//有序对分片解开读或者写锁
	for _,index:=range indices{
		_,exists:=writeIndexSet[index]
		mu:=lock.table[index]
		if exists{
			mu.Unlock()
		}else{
			mu.RUnlock()
		}
	}
}
	



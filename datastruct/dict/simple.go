package dict

import "redis/myredis/lib/wildcard"

//利用go的原生map模拟哈希表，实现一个非线程安全的哈希表，对外提供增删改查接口，遍历时允许调用方传入回调函数，决定对遍历元素的逻辑
type SimpleDict struct{
	//值为任意类型
	m map[string]interface{}
}

//构造函数创建map
func MakeSimple() *SimpleDict{
	return &SimpleDict{
		m: make(map[string]interface{}),
	}
}

//传入key，拿到value，同时返回是否存在
func (dict *SimpleDict)Get(key string)(val interface{},exists bool){
	val,exists=dict.m[key]
	return val,exists
}

//返回字典长度
func (dict *SimpleDict)Len()int{
	if dict.m==nil{
		panic("m is nil")
	}
	return len(dict.m)
}

//将key-value存入map，同时返回新增元素个数
func (dict *SimpleDict) Put(key string, val interface{}) int{
	_,exists:=dict.m[key]
	dict.m[key]=val
	//插入以前就已经存在了，返回0表示更新值
	if exists{
		return 0
	}
	return 1
}

//在不存在key的情况下才插入
func (dict *SimpleDict) PutIfAbsent (key string, val interface{}) int{
	_,exists:=dict.m[key]
	//已经存在不予插入
	if exists{
		return 0
	}
	dict.m[key]=val
	return 1
}

//在已经存在key的情况下，覆盖旧值
func (dict *SimpleDict) PutIfExists (key string, val interface{})int{
	_,exists:=dict.m[key]
	//存在时直接更新旧值
	if exists{
		dict.m[key]=val
		return 1
	}
	return 0
}

//移除key-value,返回移除的值与个数
func (dict *SimpleDict) Remove(key string) (interface{},int){
	val,exists:=dict.m[key]
	delete(dict.m,key)
	if exists{
		return val,1
	}
	return nil,0
}

//返回字典中包含的所有键,以切片的形式
func (dict *SimpleDict) Keys() []string{
	result:=make([]string,len(dict.m))
	i:=0
	//遍历map获取键值
	for k:=range dict.m{
		result[i]=k
		i++
	}
	return result
}

//传入回调函数，对map进行遍历处理，逻辑由调用方决定
func (dict *SimpleDict) ForEach(consumer Consumer){
	for k,v:=range dict.m{
		//返回false中断遍历，调用方拿到k，v以后需要返回bool决定是否继续遍历
		if !consumer(k,v){
			break
		}
	}
}

//任意拿目标个数量的键,go中的map在多次运行时，拿到的顺序可能是不一致的
func (dict *SimpleDict) RandomKeys(limit int) []string{
	result:=make([]string,limit)
	for i:=0;i<limit;i++{
		//拿到一个就推出循环，重新开始拿，因此可能拿到同样的键
		for k:=range dict.m{
			result[i]=k
			break
		}
	}
	return result
}

//任意拿去重个目标数量的键
func (dict *SimpleDict) RandomDistinctKeys(limit int) []string{
	size:=limit
	if size>len(dict.m){
		size=len(dict.m)
	}	
	result:=make([]string,size)
	i:=0
	for k:=range dict.m{
		if i==size{
			break
		}			
		result[i]=k
		i++
	}
	return result
}

//清除整个map
func (dict *SimpleDict) Clear(){
	//直接使得指针指向空map
	*dict=*MakeSimple()
}

//扫描字典,模拟redis的scan命令,根据给定的模式（pattern）筛选出匹配的键值对。
func (dict *SimpleDict) DictScan(cursor int,count int,pattern string)([][]byte,int){
	result := make([][]byte, 0)
	//首先将传入的字符串模式编译为正则表达式
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	for k := range dict.m {
		//当模式为模糊匹配或者正则表达式命中该键，那么将键值对收录
		if pattern == "*" || matchKey.IsMatch(k) {
			raw, exists := dict.Get(k)
			if !exists {
				continue
			}
			//直接将key与value类型转化为字节切片，每个字符转换为一个ascii码保存，那么0表示key，1表示value，类似的往下收集
			result = append(result, []byte(k))
			result = append(result, raw.([]byte))
		}
	}
	return result, 0
}
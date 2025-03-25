package set

import (
	"redis/myredis/datastruct/dict"
	"redis/myredis/lib/wildcard"
)

//定义一个单列集合，乱序去重,内部实质是字典接口类型，通过调用不同的构造函数可以选择创建并发安全与非并发安全的单列集合
//提供的各种增删改查接口，内部实际是调用字典接口的方法，无需关心值，实际调用的时候，会根据传入的字典类型来决定调用哪种方法

type Set struct{
	//接口类型，非并发安全、并发安全的哈希表均为该接口的实现类
	dict dict.Dict
}
//非并发安全，构造函数创建一个单列集合,可变参数传入多个元素
func Make(members ...string) *Set{
	//先创造空集合
	set:=&Set{
		dict : dict.MakeSimple(),
	}
	//将元素添加,go中的可变参数当作切片处理
	for _,member:=range members{
		set.Add(member)
	}
	return set
}
//并发安全，构造函数创建一个单列集合
func MakeConcurrentSafe(members ...string) *Set{
	set:=&Set{
		//需要传入初始的分片数量
		dict:dict.MakeConcurrent(1),
	}
	for _,member:=range members{
		set.Add(member)
	}
	return set
}
//添加元素到集合中，返回新增个数，0表示未新增
func (set *Set)Add(val string) int{
	//value值设为空，只需要key即可
	return set.dict.Put(val,nil)
}
//移除元素
func (set *Set)Remove(val string) int{
	_,ret:=set.dict.Remove(val)
	return ret
}
//查看集合是否包含某个元素
func (set *Set)Has(val string) bool{
	if set==nil || set.dict==nil{
		return false
	}
	_,ret:= set.dict.Get(val)
	return ret
}
//返回集合的元素个数
func (set *Set) Len() int{
	if set==nil||set.dict==nil{
		return 0
	}
	return set.dict.Len()
}
//遍历整个集合
func (set *Set) ForEach(consumer func(member string) bool){
	if set==nil||set.dict==nil{
		return
	}
	//调用字典的遍历方法,传入回调函数,回调函数决定遍历逻辑，并且需要返回布尔值决定是否继续往下遍历
	//在遍历函数中，会将拿到的值传给回调函数，并且对回调函数的返回值进行判断，看是否继续遍历
	set.dict.ForEach(func(key string,val interface{}) bool{
		return consumer(key)
	})
}
//将单列集合的键集合转化为切片
func (set *Set) ToSlice() []string{
	slice:=make([]string,set.Len())
	i:=0
	set.dict.ForEach(func (key string,val interface{})bool{
		if i<len(slice){
			slice[i]=key
		}else{
			slice=append(slice,key)
		}
		i++
		return true
	})
	return slice
}
//浅拷贝,拿到单列集合中所有的元素
func (set *Set) ShallowCopy() *Set{
	result:=Make()
	set.ForEach(func(val string)bool{
		result.Add(val)
		return true
	})
	return result
}
//求单列集合的交集,可传入多个集合
func Intersect(set ...*Set) *Set{
	result:=Make()
	if len(set)==0{
		return result
	}
	countMap:=make(map[string]int)
	//遍历拿到的集合切片
	for _,s:=range set{
		//调用ForEach遍历每个集合中的元素
		//形参传入一个回调函数，在foreach内部，拿到这个形参以后会调用这个回调函数，将
		//遍历到的元素传给这个回调函数，然后会检查回调的返回值，如果返回true，则继续遍历，如果返回false，则停止遍历
		s.ForEach(func(member string)bool{
			countMap[member]++
			return true
		})
	}
	//再遍历map，获取每个key的元素个数，如果等于set集合的长度，那么说明所有集合都有该元素
	for k,v:=range countMap{
		if v==len(set){
			result.Add(k)
		}
	}
	return result
}
//求单列集合的并集
func Union(set ...*Set) *Set{
	result:=Make()
	if len(set)==0{
		return result
	}
	//先拿集合切片遍历
	for _,s:=range set{
		//再遍历每个集合的元素
		s.ForEach(func(member string)bool{
			//每个元素都加到结果集合当中
			result.Add(member)
			//总是继续往下遍历，拿所有元素
			return true
		})
	}
	return result
}
//求两个集合的差异，实质就是a-a交b
func Diff(set ...*Set) *Set{
	if len(set)==0{
		return Make()
	}
	//拿到第一个集合的所有元素
	result:=set[0].ShallowCopy()
	//遍历第一个集合，剔除第二个集合也拥有的元素
	for i:=1;i<len(set);i++{
		//member接到第二个集合元素，直接在结果集合中剔除，并返回true表示一直往下遍历
		set[i].ForEach(func(member string)bool{
			result.Remove(member)
			return true
		})
		//如果结果集合为空，直接返回
		if result.Len()==0{
			break
		}
	}
	return result
}
//传入期望值，以字符串切片形式返回任意个集合内的元素，可能包含重复的元素
func (set *Set)RandomKeys(limit int) []string{
	return set.dict.RandomKeys(limit)
}
//不包含重复的元素，返回随机个数元素
func (set *Set)RandomDistinctKeys(limit int)[]string{
	return set.dict.RandomDistinctKeys(limit)
}
//全量扫描，获取整个集合中与模式匹配的字符串
func (set *Set)SetScan(cursor int,count int,pattern string)([][]byte,int){
	result:=make([][]byte,0)
	//将字符串模式编译为正则表达式
	matchKey,err:=wildcard.CompilePattern(pattern)
	if err!=nil{
		return result,-1
	}
	set.ForEach(func(member string)bool{
		if pattern=="*"||matchKey.IsMatch(member){
			result=append(result,[]byte(member))
		}
		return true
	})
	//返回0表示整个集合全部扫描完成，下一次起始位置又从初始位置开始。返回-1表示出错
	return result,0
}
/**增量对集合扫描,传入游标位置，期望获取的数目（不一定能拿到这么多），匹配模式，返回匹配到的键和下次扫描的游标位置
func (set *Set)SetScan(cursor int,count int,pattern string)([][]byte,int){
	result:=make([][]byte,0)
	//将字符串模式编译为正则表达式
	matchKey,err:=wildcard.CompilePattern(pattern)
	if err!=nil{
		return result,-1
	}
	members:=set.ToSlice()
	total:=set.Len()
	//游标位置不合法
	if cursor>=total{
		return result,-1
	}
	for i:=cursor;i<total;i++{
		if pattern=="*"||matchKey.IsMatch(members[i]){
			result=append(result,[]byte(members[i]))
		}
		if len(result)>=count{
			return result,i+1
		}
	}
	return result,0
}
**/

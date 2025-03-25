package list

//list对外提供的接口
//遍历函数，传入期望值，返回是否匹配，决定是否作相应的操作
type Expected func(a interface{}) bool
//回调函数，传入索引与节点值，继续遍历返回true，否则停止遍历返回false
type Consumer func(i int,v interface{}) bool

//list接口
type List interface{
	Add(val interface{})
	Get(index int) (val interface{})
	Set(index int, val interface{})
	Insert(index int, val interface{})
	Remove(index int) (val interface{})
	RemoveLast() (val interface{})
	RemoveAllByVal(expected Expected) int
	RemoveByVal(expected Expected, count int) int
	ReverseRemoveByVal(expected Expected, count int) int
	Len() int
	ForEach(consumer Consumer)
	Contains(expected Expected) bool
	Range(start int, stop int) []interface{}
}


package list

/*
	双向链表定义,包含首位指针，以及节点总数
*/

//包含首尾指针的双向链表结构，且无首尾哑节点
type LinkedList struct{
	first *node
	last *node
	size int
}
//可以接受任意类型的元素
type node struct{
	val interface{}
	prev *node
	next *node
}
//在链表尾部添加节点
func (list *LinkedList) Add(val interface{}){
	if list==nil{
		panic("list is nil")
	}
	n:=&node{
		val:val,
	}
	if list.last==nil{
		list.last=n
		list.first=n
	}else{
		list.last.next=n
		n.prev=list.last
		list.last=n
	}
	list.size++
}
//查找节点,返回该节点的地址
func (list *LinkedList) find(index int) (n *node){
	if index<list.size/2{
		n=list.first
		for i:=0;i<index;i++{
			n=n.next
		}
	}else{
		n=list.last
		for i:=list.size-1;i>index;i--{
			n=n.prev
		}
	}
	return n 
}
//获取节点的值
func (list *LinkedList) Get(index int)(val interface{}){
	if list==nil{
		panic("list is nil")
	}
	if index<0 || index>=list.size{
		panic("index out of boud")
	}
	return list.find(index).val
}
//修改节点值
func (list *LinkedList) Set(index int, val interface{}){
	if list==nil{
		panic("list is nil")
	}
	if index<0|| index>list.size{
		panic("index out of boud")
	}
	n:=list.find(index)
	n.val=val
}
//在指定位置的前面添加节点
func (list *LinkedList) Insert(index int,val interface{}){
	if list==nil{
		panic("list is nil")
	}
	if index<0||index>list.size{
		panic("index out of boud")
	}
	if index==list.size{
		list.Add(val)
		return
	}
	pivot:=list.find(index)
	n:=&node{
		val:val,
		prev:pivot.prev,
		next:pivot,
	}
	if pivot.prev==nil{
		list.first=n
	}else{
		pivot.prev.next=n
	}
	pivot.prev=n
	list.size++
}
//直接根据某个节点指针，移除某个节点，应注意本节点是否为首尾节点
func (list *LinkedList) removeNode(n *node){
	if n.prev==nil{
		list.first=n.next
	}else{
		n.prev.next=n.next
	}
	if n.next==nil{
		list.last=n.prev
	}else{
		n.next.prev=n.prev
	}
	n.prev=nil
	n.next=nil
	list.size--
}
//指定下标，移除节点并返回该节点的值
func (list *LinkedList) Remove(index int) (val interface{}){
	if list==nil{
		panic("list is nil")
	}
	if index<0||index>=list.size{
		panic("index out of bound")
	}
	n:=list.find(index)
	list.removeNode(n)
	return n.val
}
//移除末尾节点，返回节点值
func (list *LinkedList) RemoveLast()(val interface{}){
	if list==nil{
		panic("list is nil")
	}
	if list.last==nil{
		return nil
	}
	n:=list.last
	list.removeNode(n)
	return n.val
}
//传入期望值，遍历整个链表删除链表中所有的目标节点,并返回删除个数
func (list *LinkedList) RemoveAllByVal(expected Expected) int{
	if list==nil{
		panic("list is nil")
	}
	n:=list.first
	removed:=0
	var nextNode *node
	for n!=nil{
		nextNode=n.next
		if expected(n.val){
			list.removeNode(n)
			removed++
		}
		n=nextNode
	}
	return removed
}
//传入期望值与期望删除的个数，从左往右遍历整个链表删除链表中所有的目标节点,并返回删除个数
func (list *LinkedList) RemoveByVal(expected Expected,count int) int{
	if list==nil{
		panic("list is nil")
	}
	n:=list.first
	removed:=0
	var nextNode *node
	for n!=nil{
		nextNode=n.next
		if expected(n.val){
			list.removeNode(n)
			removed++
		}
		if count==removed{
			break
		}
		n=nextNode
	}
	return removed
}
//传入期望值与期望删除的个数，从右边往左边遍历整个链表删除链表中所有的目标节点,并返回删除个数
func (list *LinkedList) ReverseRemoveByVal(expected Expected,count int) int{
	if list==nil{
		panic("list is nil")
	}
	n:=list.last
	removed:=0
	var prevNode *node
	for n!=nil{
		prevNode=n.prev
		if expected(n.val){
			list.removeNode(n)
			removed++
		}
		if count==removed{
			break
		}
		n=prevNode
	}
	return removed
}
func (list *LinkedList) Len() int{
	if list==nil{
		panic("list is nil")
	}
	return list.size
}
//传入回调函数，遍历整个链表，由调用方决定对遍历到的范围进行处理逻辑
func (list *LinkedList) ForEach(consumer Consumer){
	if list==nil{
		panic("list is nil")
	}
	n:=list.first
	i:=0
	for n!=nil{
		if !consumer(i,n.val){
			break
		}
		i++
		n=n.next
	}
}
//查看链表中是否有给定值
func (list *LinkedList) Contains(expected Expected) bool{
	contains:=false
	list.ForEach(func(i int,v interface{}) bool{
		if expected(v){
			contains=true
			return false
		}
		return true
	})
	return contains
}
//返回一定范围下的元素值，装入容器中，注意不包含stop结束位置
func (list *LinkedList) Range(start,stop int) []interface{}{
	if list==nil{
		panic("list is nil")
	}
	if start<0||start>=list.size{
		panic("`start` out of bound")
	}
	if stop>list.size||stop<start{
		panic("`stop` out of bound")
	}
	slice_size:=stop-start
	slice:=make([]interface{},slice_size)
	n:=list.first
	i:=0
	sliceIndex:=0
	for n!=nil{
		if i >= start && i < stop {
			slice[sliceIndex] = n.val
			sliceIndex++
		} else if i >= stop {
			break
		}
		i++
		n = n.next
	}
	return slice
}
//传入一些元素创建一个新链表
func Make(vals ...interface{}) *LinkedList{
	list:=&LinkedList{}
	for _,val:=range vals{
		list.Add(val)
	}
	return list
}

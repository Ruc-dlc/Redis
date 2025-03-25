package list

import "container/list"

//分页大小
const pageSize=1024
//快表，在传统链表的基础上进行优化，通过分页的形式加快访问速度，同时在内存使用上做优化
type QuickList struct{
	data *list.List
	size int //对应快表内保存的元素个数，注意不是页面个数
}
//定义迭代器，通过迭代器可以拿到该节点的页内偏移量为offset的元素
type iterator struct{
	node *list.Element
	offset int
	ql *QuickList
}
//构造函数创建一个新的快表对象
func NewQuickList() *QuickList{
	l:=&QuickList{
		data:list.New(),
		size:0,
	}
	return l
}
//往快表的最后面插入元素
func (ql *QuickList) Add(val interface{}){
	ql.size++
	//如果快表一页都没有，那么就创建新页面
	if ql.data.Len()==0{
		page:=make([]interface{},0,pageSize)
		page=append(page,val)
		ql.data.PushBack(page)
		return 
	}
	//如果快表已经有页面了，那么往最后一页里面添加元素
	backNode:=ql.data.Back()
	backPage:=backNode.Value.([]interface{})
	//有页面，但是最后一页容量满了，那么就创建新页面
	if len(backPage)==cap(backPage){
		page:=make([]interface{},0,pageSize)
		page=append(page,val)
		ql.data.PushBack(page)
		return
	}
	//上述都不满足直接往最后一页添加元素
	backPage=append(backPage,val)
	backNode.Value=backPage
}
//传入下标，找到该元素存放的页面（节点）,以及该下标在本页的偏移量，返回迭代器
func (ql *QuickList) find(index int) *iterator{
	if ql==nil{
		panic("list is nil")
	}
	if index<0||index>=ql.size{
		panic("index out of bound")
	}
	//定义节点与页面与页面的第一个元素对应的下标
	var n *list.Element
	var page []interface{}
	var pageBeg int
	//从表的前面往后面寻找
	if index<ql.size/2{
		n=ql.data.Front()
		pageBeg=0
		for{
			//类型断言，当不是切片时，引发panic
			page=n.Value.([]interface{})
			if pageBeg+len(page)>index{
				break
			}
			pageBeg+=len(page)
			n=n.Next()
		}
	}else{
		//从表的最后往前面寻找
		n=ql.data.Back()
		pageBeg=ql.size
		for{
			page=n.Value.([]interface{})
			pageBeg-=len(page)
			if pageBeg<=index{
				break
			}
			n=n.Prev()
		}
	}
	return &iterator{
		node:n,
		offset:index-pageBeg,
		ql:ql,
	}
}
//通过迭代器拿到本节点内保存的页面（元素切片）
func (it *iterator) page() []interface{}{
	return it.node.Value.([]interface{})
}
//通过迭代器先拿到页面然后在拿到偏移量对应的元素值
func (it *iterator) get() interface{}{
	return it.page()[it.offset]
}
//查看当前迭代器对应的元素下一个元素是否存在,同时设置迭代器指向下一个元素
func (it *iterator) next() bool{
	page:=it.page()
	//下一个位置在本页面当中
	if it.offset+1<len(page){
		it.offset++
		return true
	}
	if it.node==it.ql.data.Back(){
		it.offset=len(page)
		return false
	}
	//下一个位置不在本页面当中
	it.node=it.node.Next()
	it.offset=0
	return true
}
//查看当前迭代器对应的元素上一个元素是否存在,同时设置迭代器指向下一个元素
func (it *iterator) prev() bool{
	//上一个位置在本页面当中
	if it.offset>0{
		it.offset--
		return true
	}
	if it.node==it.ql.data.Front(){
		it.offset=-1
		return false
	}
	//上一个位置不在本页面当中
	it.node=it.node.Prev()
	page:=it.page()
	it.offset=len(page)-1
	return true
}
//查看当前迭代器是否到了快表的末尾，可用于遍历查看迭代器是否遍历到了末尾
func (it *iterator) atEnd() bool{
	if it.ql.data.Len()==0{
		return true
	}
	if it.node!=it.ql.data.Back(){
		return false
	}
	page:=it.page()
	return it.offset==len(page)
}
//查看当前迭代器是否到了快表的头部，可用于遍历查看迭代器是否遍历到了头部
func (it *iterator) atBegin() bool{
	if it.ql.data.Len()==0{
		return true
	}
	if it.node!=it.ql.data.Front(){
		return false
	}
	return it.offset==-1
}
//传入index获取元素值
func (ql *QuickList) Get(index int) interface{}{
	it:=ql.find(index)
	return it.get()
}
//修改迭代器指向的元素值
func (it *iterator) set(val interface{}){
	it.page()[it.offset]=val
}
//传入index修改快表中该位置的元素值
func (ql *QuickList) Set(index int, val interface{}){
	it:=ql.find(index)
	it.page()[it.offset]=val
}
//在指定的index处插入元素
func (ql *QuickList) Insert(index int,val interface{}){
	if index==ql.size{
		ql.Add(val)
		return 
	}
	it:=ql.find(index)
	page:=it.page()
	if len(page)<pageSize{
		//当前页还没有装满
		page=append(page[:it.offset+1],page[it.offset:]...)
		page[it.offset]=val
		it.node.Value=page
		ql.size++
		return
	}
	//当前页面已经装满，采用将当前页面所有元素分裂为两个页面内放置
	var nextPage []interface{}
	nextPage = append(nextPage, page[pageSize/2:]...) // pageSize must be even
	page = page[:pageSize/2]
	//偏移量在本页面前半页内部
	if it.offset < len(page) {
		page = append(page[:it.offset+1], page[it.offset:]...)
		page[it.offset] = val
	} else {
		//在后半页
		i := it.offset - pageSize/2
		nextPage = append(nextPage[:i+1], nextPage[i:]...)
		nextPage[i] = val
	}
	it.node.Value = page
	//在迭代器指向的节点后面添加新页面
	ql.data.InsertAfter(nextPage, it.node)
	ql.size++
}
//传入迭代器，删除迭代器所指的元素,同时移动迭代器到下一个位置
func (it *iterator) remove() interface{}{
	page := it.page()
	val := page[it.offset]
	page = append(page[:it.offset], page[it.offset+1:]...)
	if len(page) > 0 {
		//当前页删除了以后仍不为空
		it.node.Value = page
		if it.offset == len(page) {
			//删除的不是最后一个节点的最后一个元素，把迭代器移动到下一页的第一个位置
			if it.node != it.ql.data.Back() {
				it.node = it.node.Next()
				it.offset = 0
			}
		}
	} else {
		//页面本来只有一个元素，删光了
		if it.node == it.ql.data.Back() {
			//如果删除的是最后一页，将最后一页空间回收，并移动迭代器到上一页的最后一个位置的后面一个位置
			if prevNode := it.node.Prev(); prevNode != nil {
				it.ql.data.Remove(it.node)
				it.node = prevNode
				it.offset = len(prevNode.Value.([]interface{}))
			} else {
				//前面没有节点了，说明删除的是唯一的一个节点，直接将迭代器置为空
				it.ql.data.Remove(it.node)
				it.node = nil
				it.offset = 0
			}
		} else {
			//删除的不是最后一个节点，往后挪动一个页面
			nextNode := it.node.Next()
			it.ql.data.Remove(it.node)
			it.node = nextNode
			it.offset = 0
		}
	}
	it.ql.size--
	return val
}
//移除下标index的元素
func (ql *QuickList) Remove(index int) interface{}{
	it:=ql.find(index)
	return it.remove()
}
//返回快表的元素总个数
func (ql *QuickList) Len() int {
	return ql.size
}
//删除最后一个元素，并返回该元素值
func (ql *QuickList) RemoveLast() interface{} {
	if ql.Len() == 0 {
		return nil
	}
	ql.size--
	lastNode := ql.data.Back()
	lastPage := lastNode.Value.([]interface{})
	if len(lastPage) == 1 {
		ql.data.Remove(lastNode)
		return lastPage[0]
	}
	val := lastPage[len(lastPage)-1]
	lastPage = lastPage[:len(lastPage)-1]
	lastNode.Value = lastPage
	return val
}
//从左往右边，删除传入的期望值，并返回一共找到多少个
func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}
	return removed
}
//传入期待删除值，遍历整个快表，删除count个元素，并返回实际一共找到多少个
func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		} else {
			iter.next()
		}
	}
	return removed
}
//从右往左，删除传入的期望值，并返回一共找到多少个
func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(ql.size - 1)
	removed := 0
	for !iter.atBegin() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		}
		iter.prev()
	}
	return removed
}
//在遍历函数中传入回调函数，使调用方决定遍历到的元素的执行逻辑,内部在合适的地方对回调函数进行判断，决定是否要继续遍历
//当回调函数返回真表示继续往下遍历，否则中断遍历
func (ql *QuickList) ForEach(consumer Consumer) {
	if ql == nil {
		panic("list is nil")
	}
	if ql.Len() == 0 {
		return
	}
	iter := ql.find(0)
	i := 0
	for {
		goNext := consumer(i, iter.get())
		if !goNext {
			break
		}
		i++
		//移动到下一个下标位置
		if !iter.next() {
			break
		}
	}
}
//查看快表是否包含传入的期望值
func (ql *QuickList) Contains(expected Expected) bool {
	contains := false
	ql.ForEach(func(i int, actual interface{}) bool {
		if expected(actual) {
			contains = true
			return false//中断遍历
		}
		return true//继续往下遍历
	})
	return contains
}
// 返回一定范围下面的元素值，范围为[start,stop)
func (ql *QuickList) Range(start int, stop int) []interface{} {
	if start < 0 || start >= ql.Len() {
		panic("`start` out of range")
	}
	if stop < start || stop > ql.Len() {
		panic("`stop` out of range")
	}
	//确定传入范围的切片元素总量
	sliceSize := stop - start
	slice := make([]interface{}, 0, sliceSize)
	it := ql.find(start)//找第一个元素所在的页面
	i := 0
	for i < sliceSize {
		slice = append(slice, it.get())
		it.next()
		i++
	}
	return slice
}
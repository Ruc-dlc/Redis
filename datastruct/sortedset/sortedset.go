package sortedset

import (
	"strconv"
	"redis/myredis/lib/wildcard"
)
//有序集合本身，底层是一个跳表用来存储member成员和分数，设置一个哈希表来存储成员和分数的对应关系
type SortedSet struct {
	//哈希表额外存一下成员，可以以o(1)时间复杂度查找
	dict     map[string]*Element
	//有序链表用于存放member成员和分数，跳表是按照分数从小到大排序的，效率为对数级别O(logN)
	skiplist *skiplist
}

//构造函数创建一个有序集合
func Make() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: makeSkiplist(),
	}
}

//传入member成员和分数，往有序集合里插入一个节点，返回true表示新插入了一个节点，返回false表示更新了member的分数
func (sortedSet *SortedSet) Add(member string, score float64) bool {
	//检查之前是否已经有该member了，如果已经存在，更新分数，否则插入新节点
	element, ok := sortedSet.dict[member]
	//不论之前是否存在，都把member插入到哈希表中
	sortedSet.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		//若之前已经存在该member并且分数不等于此次传入的分数，那么就更新旧分数
		if score != element.Score {
			//先移除旧节点，然后添加新节点
			sortedSet.skiplist.remove(member, element.Score)
			sortedSet.skiplist.insert(member, score)
		}
		return false
	}
	sortedSet.skiplist.insert(member, score)
	return true
}

//直接查看哈希表内存的member个数返回有序集合中的所有元素
func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict))
}
//传入member查询对应节点的元素（包含member成员和对应分数）
func (sortedSet *SortedSet) Get(member string) (element *Element, ok bool) {
	//检查member成员是否存在
	element, ok = sortedSet.dict[member]
	if !ok {
		//如果有序集合中没有,直接返回查找失败
		return nil, false
	}
	//返回查询到的元素和查询成功状态
	return element, true
}
//给定member，从有序集合中移除元素
func (sortedSet *SortedSet) Remove(member string) bool {
	//检查member成员是否存在	
	element, ok := sortedSet.dict[member]
	if ok {
		//存在调用跳表删除接口，并更新哈希表
		sortedSet.skiplist.remove(member, element.Score)
		delete(sortedSet.dict, member)
		return true
	}
	//不存在
	return false
}
//传入member和排名方式，desc为true表示降序排名也就是从小到大，false表示升序排名
func (sortedSet *SortedSet) GetRank(member string, desc bool) (rank int64) {
	//检查哈希表是否存在该元素
	element, ok := sortedSet.dict[member]
	if !ok {
		//不存在直接返回-1错误
		return -1
	}
	//先拿到升序排名，跳表的哑节点排名为0，第一个节点为1，以此类推，其实r就是数据节点的个数了
	r := sortedSet.skiplist.getRank(member, element.Score)
	//如果是拿降序排名，那么需要用表长减掉前面节点的个数，得到降序排名
	if desc {
		r = sortedSet.skiplist.length - r
	} else {
		//返回升序排名，由于从0开始排名，所以需要减1
		r--
	}
	return r
}

// 传入 [start, stop)排名区间, 允许指定升序或降序, 排名从0开始，传入回调函数，由调用方定义对处于排名区间的元素作何种操作
func (sortedSet *SortedSet) ForEachByRank(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := sortedSet.Len()
	//检查排名区间是否合法，经检验以后，跳表内就一定有该区间的节点
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop <= start || stop > size {
		panic("illegal end " + strconv.FormatInt(stop, 10))
	}
	//开始找指定区间的第一个节点，也就是start对应的节点
	var node *node
	if desc {
		//如果是降序排名区间，那么直接从尾部开始寻找，因为尾部是最大的元素
		node = sortedSet.skiplist.tail
		if start > 0 {
			//调用根据排名获取节点的接口，拿到第一个节点，由于跳表提供的接口需要传入的是升序的排名，所以传入的是size-start
			node = sortedSet.skiplist.getByRank(size - start)
		}
	} else {
		node = sortedSet.skiplist.header.level[0].forward
		if start > 0 {
			//升序排名，直接传入start+1，由于跳表的接口需要加上哑节点
			node = sortedSet.skiplist.getByRank(start + 1)
		}
	}
	//拿到start排名节点后，开始遍历找排名范围内的元素
	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		//拿到目标排名的元素值，作操作，并返回布尔值，提供是否继续往下遍历，返回false表示终止遍历
		if !consumer(&node.Element) {
			break
		}
		//如果是降序，那么需要一直往前驱节点走
		if desc {
			node = node.backward
		} else {
			//如果是升序，那么需要一直往后继节点走
			node = node.level[0].forward
		}
	}
}

// 返回排名区间在[start, stop)内的元素值, 跳表以升序排列, 排名从0开始
func (sortedSet *SortedSet) RangeByRank(start int64, stop int64, desc bool) []*Element {
	//定义结果切片
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	//直接调用遍历函数，获取到处于排名区间的节点，将元素值收集到结果集中
	sortedSet.ForEachByRank(start, stop, desc, func(element *Element) bool {
		//回调函数接到遍历函数传入的元素值，直接存入结果集
		slice[i] = element
		i++
		//由于需要找到所有位于区间内的节点，于是总是返回true继续遍历
		return true
	})
	return slice
}

//计算有序集合中处于给定上下界排名节点的个数
func (sortedSet *SortedSet) RangeCount(min Border, max Border) int64 {
	var i int64 = 0
	//排名区间传入是升序排名区间，从0开始遍历到表长位置
	sortedSet.ForEachByRank(0, sortedSet.Len(), false, func(element *Element) bool {
		//找第一个大于等于min边界的节点
		gtMin := min.less(element) 
		if !gtMin {
			return true
		}
		//检查找到的节点是否小于等于上界
		ltMax := max.greater(element) 
		if !ltMax {
			//如果发现节点的排名已经要比上界大了，那么终止遍历
			return false
		}
		//找到的目标值处于排名区间内，累计起来，并继续遍历下一个排名的节点
		i++
		return true
	})
	return i
}

// 遍历访问成员或者分数区间内的元素，允许指定升序或降序，允许指定偏移量，允许指定返回个数
func (sortedSet *SortedSet) ForEach(min Border, max Border, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	// 找第一个节点
	var node *node
	if desc {
		//降序的话，是拿到边界范围内的最后一个节点，该节点元素值时最大的
		node = sortedSet.skiplist.getLastInRange(min, max)
	} else {
		node = sortedSet.skiplist.getFirstInRange(min, max)
	}
	//若指定了偏移量，那么将开始节点移动到从偏移量开始
	for node != nil && offset > 0 {
		if desc {
			//降序往前驱走
			node = node.backward
		} else {
			//升序往后继走
			node = node.level[0].forward
		}
		offset--
	}
	//指定个数如果传入的是负值，那么表示没有个数限制也就是整表，否则表示最多返回limit个元素
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		//每次拿到的值，必须在边界范围内，才有效，否则直接跳出循环
		gtMin := min.less(&node.Element) 
		ltMax := max.greater(&node.Element)
		if !gtMin || !ltMax {
			break 
		}
	}
}
//拿到上下界范围内的指定数量个元素合集，允许指定升序或降序，允许指定偏移量，允许指定返回个数
func (sortedSet *SortedSet) Range(min Border, max Border, offset int64, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	sortedSet.ForEach(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}
//移除给定上下界范围内的元素，并返回被移除的元素个数
func (sortedSet *SortedSet) RemoveRange(min Border, max Border) int64 {
	removed := sortedSet.skiplist.RemoveRange(min, max, 0)
	for _, element := range removed {
		//移除节点需要更新哈希表
		delete(sortedSet.dict, element.Member)
	}
	//返回更新的元素的个数
	return int64(len(removed))
}
//移除有序集合中，count个数量较小的节点
func (sortedSet *SortedSet) PopMin(count int) []*Element {
	//传入负无穷大和正无穷大作为上下界，表示取整个表内的第一个最小的元素节点
	first := sortedSet.skiplist.getFirstInRange(scoreNegativeInfBorder, scorePositiveInfBorder)
	if first == nil {
		return nil
	}
	//构造一个下限边界
	border := &ScoreBorder{
		Inf:0,
		Value:   first.Score,
		Exclude: false,
	}
	//调用跳表移除排名区间的接口，移除count个节点
	removed := sortedSet.skiplist.RemoveRange(border, scorePositiveInfBorder, count)
	for _, element := range removed {
		//移除元素的同时需要更新哈希表
		delete(sortedSet.dict, element.Member)
	}
	return removed
}
//移除有序集合中，位于 [start, stop)排名范围的节点
// 跳表是从小到大排序组织，另外排名从0开始
func (sortedSet *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	//由于跳表的接口需要算上哑节点，所以要给传入的参数增加1
	removed := sortedSet.skiplist.RemoveRangeByRank(start+1, stop+1)
	for _, element := range removed {
		//更新哈希表
		delete(sortedSet.dict, element.Member)
	}
	//返回删掉的节点个数
	return int64(len(removed))
}
//遍历sortedSet的成员，返回匹配模式条件的成员
func (sortedSet *SortedSet) ZSetScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	//将字符串模式编译为正则表达式
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	//开始遍历有序集合重的哈希表，拿到的是键
	for k := range sortedSet.dict {
		if pattern == "*" || matchKey.IsMatch(k) {
			//匹配成功，再检查一遍是否存在
			element, exists := sortedSet.dict[k]
			if !exists {
				continue
			}
			//开始收集答案，将member成员和分数依次加入到结果集中，换句话说，每两行是一个元素，第一行是member，第二行是分数
			result = append(result, []byte(k))
			result = append(result, []byte(strconv.FormatFloat(element.Score, 'f', 10, 64)))
		}
	}
	return result, 0
}

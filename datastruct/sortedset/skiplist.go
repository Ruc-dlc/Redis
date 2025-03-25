package sortedset


//跳表的增删改查接口，为后续实现排序集合而设计，关键结构体有元素、层级信息、跳表本身、节点
//策略就是使用概率性的链表，使得每个节点出现在上层的概率是下层的一半，如此一来，上层索引比传统
//的链表来讲，可以快速的跳过一些节点，定位到目标的范围，时间复杂度从O(n)降低到O(logn)，使用的时候
//总是从上到下，从左到右去遍历

import (
	"math/bits"
	"math/rand"
)

//限制跳表的最大层数为16
const (
	maxLevel = 16
)

//元素类型，包含成员本身及其分数用于排序，member唯一，但score可以不唯一可以存在相同的分数
type Element struct{
	Member string
	Score float64
}

//节点的高层索引结构体，包含每个节点的层级信息,包含后继指针与跨过的节点数目
//节点访问层级信息，可以方便的访问到某一层的后继节点，以及跨过的节点数目
type Level struct{
	forward *node
	//span字段的设置主要是为了加速按照排序查询，该跨度表示在第0层，下一个节点与该节点之间间隔的节点个数
	span int64 //后期与排名相关的接口可以利用到该字段从线性时间复杂度提升到对数级别效率
}

//定义跳表的节点,包含元素，第0层的前驱指针与节点的高层索引信息
type node struct{
	Element
	//节点在第0层的前驱节点位置指针
	backward *node
	level []*Level
}

//跳表定义，包含头尾指针分别指向头尾节点、跳表节点数量与跳表当前的层级
type skiplist struct{
	//头节点作为哑节点，不存储实际数据，增删改查的入口是skiplist.header.level[level]
	header *node
	//指向跳表真实节点的最后一个
	tail *node
	length int64
	level int16
}

//新建一个节点，需要传入的信息包含该节点需要出现在0-level-1层，分数，值本身，同时会
//创建等层级数量个用于保存层级信息的堆变量，节点可以通过层级切片指针访问
func makeNode(level int16, score float64, member string) *node{
	//一个节点需要做两件事，赋元素值及其分数、初始化节点层级信息也就是该节点出现在0～level-1层
	n:=&node{
		Element: Element{
			Member:member,
			Score:score,
		},
		level:make([]*Level, level),
	}
	for i:=range n.level{
		n.level[i]=new(Level)
	}
	return n
}

//创建一个跳表，初始层级为1，头节点为哑节点
func makeSkiplist() *skiplist{
	return &skiplist{
		level:1,
		//头节点作为哑节点，包含当前最大允许层次的层级结构体，分数为0，值为空
		//因为没有实际节点，所以对尾节点不作初始化
		header: makeNode(maxLevel, 0, ""),
	}
}

//创建节点时，为节点赋一个能够出现的层级，概率模型，可以保证以一半的概率出现在上层
func randomLevel() int16{
	total:=uint64(1)<<uint64(maxLevel) - 1
	k:=rand.Uint64()%total
	return maxLevel-int16(bits.Len64(k+1)) + 1
}

//为跳表插入一个节点，并返回新节点的指针
func (skiplist *skiplist) insert(member string, score float64) *node{
	//记录插入节点的前置节点
	update:=make([]*node, maxLevel)
	//用于存放待插入节点的前驱节点的排名，0是第一名，数值可以直接理解为数组的索引
	rank:=make([]int64, maxLevel)
	//找到对应的位置进行插入,从最高位置往下面逐层找
	//拿到跳表的哑节点，就是当前遍历到的节点
	node:=skiplist.header
	//从当前最高层往下到第0层为止
	for i:=skiplist.level-1;i>=0;i--{	
		//最高层的排名初始化为0
		if i==skiplist.level-1{
			rank[i]=0
		}else{
			//除去最高层，都是继承上一层的初始排名
			rank[i]=rank[i+1]
		}
		//如果每一层的起始节点的层级信息不为空，说明在该层还有下一个节点
		if node.level[i]!=nil{
			//从左往右遍历，首先下一个节点不为空，并且下一个节点的分数低于待插入分数，分数相同的话，按照值的字典序从小到大排序，就往后走
			for node.level[i].forward!=nil && 
			(node.level[i].forward.Score<score ||(node.level[i].forward.Score==score && node.level[i].forward.Member<member)){
				//每次往后走之前，会把当前层当前节点的下一个节点的距离加到排名上
				rank[i]+=node.level[i].span
				//同层次，从左往右走
				node=node.level[i].forward
			}
		}
		//跳出循环以后，起始就已经找到了目标节点应该放置到该层的一个位置了，记录下此时的前驱节点
		update[i]=node
	}
	//随机生成一个该节点应该出现的level层级
	level:=randomLevel()
	//如果新节点层级比当前最大层级还要大
	if level>skiplist.level{
		//从当前最高层开始往上直到新节点的层级，目标节点的前驱节点都是哑节点，跨度为整个跳表的实际节点数目
		for i:=skiplist.level;i<level;i++{
			rank[i]=0
			update[i]=skiplist.header
			//后面没有节点，直接把跨度设置为表长
			update[i].level[i].span=skiplist.length
		}
		//更新当前跳表最高层级
		skiplist.level=level
	}
	//开始创建新节点，并更新前驱后继位置关系与前驱节点和新节点某层的跨度大小
	node=makeNode(level,score,member)
	//更新的时候从下往上开始
	for i:=int16(0);i<level;i++{
		//更新节点的后继节点
		node.level[i].forward=update[i].level[i].forward
		//更新前驱节点的后继节点为新节点
		update[i].level[i].forward=node
		//更新新节点的跨度，等于原本的跨度除去前驱节点到第0层时前驱节点的跨度
		node.level[i].span=update[i].level[i].span-(rank[0]	-rank[i])
		//更新前驱节点跨度
		update[i].level[i].span=rank[0]+1-rank[i]
	}
	//新节点以上的节点到最高层位置的前驱节点的跨度，需要自增1
	for i:=level;i<skiplist.level;i++{
		update[i].level[i].span++
	}
	//开始处理新节点在第0层的时候的前驱后继关系，以及跳表的尾指针
	//0层往上包括level信息，都需要更新后继位置，但是第0层的前驱节点需要特殊处理
	if update[0] == skiplist.header {
		//加入的节点是第一个节点，前驱记为空
		node.backward=nil
	}else{
		node.backward=update[0]
	}
	if node.level[0].forward!=nil{
		//更改新节点的后继节点的前驱节点为新节点
		node.level[0].forward.backward=node
	}else{
		//跳表的尾节点指向该新节点，因为新节点是最后一个节点
		skiplist.tail=node
	}
	skiplist.length++
	return node
}

//直接传入待移除的节点的指针地址，与待删除的前驱节点，注意可能前驱节点的下一个节点不是待删除节点
func (skiplist *skiplist) removeNode(node *node, update []*node){
	for i:=int16(0);i<skiplist.level;i++{
		if update[i].level[i].forward==node{
			update[i].level[i].span+=node.level[i].span-1
			update[i].level[i].forward=node.level[i].forward
		}else{
			update[i].level[i].span--
		}
	}
	//处理节点的0层的前驱关系
	if node.level[0].forward!=nil{
		//删除节点不是最后一个节点
		node.level[0].forward.backward=node.backward
	}else{
		skiplist.tail=node.backward
	}
	//检查哑节点后面是否为空
	for skiplist.level>1&&skiplist.header.level[skiplist.level-1].forward==nil{
		skiplist.level--
	}
	skiplist.length--
}

//传入待删除节点的分数与值,返回删除是否成功
func (skiplist *skiplist) remove(member string, score float64) bool{
	update:=make([]*node,maxLevel)
	node:=skiplist.header
	for i:=skiplist.level-1;i>=0;i--{
		if node.level[i]!=nil{
			//从左往右边找，找到待删除节点的前驱节点，按照分数从小到大，分数相同按照字典序从小到大
			for node.level[i].forward!=nil&&(node.level[i].forward.Score<score||
			node.level[i].forward.Score==score&&node.level[i].forward.Member<member){
				node=node.level[i].forward
			}
		}
		update[i]=node
	}
	//退出循环时，node节点为待删除的前驱节点，往后走一步拿到第0层的待删除的节点
	node=node.level[0].forward
	//如果发现待删除节点非空,且正好是待删除节点那么调用删除接口，返回true
	if node!=nil&&node.Score==score&&node.Member==member{
		skiplist.removeNode(node, update)
		return true
	}
	return false
}

//查找分数与值，返回该节点的排名，其实就是第0层的一个索引,该接口返回的是升序排名
func (skiplist *skiplist) getRank(member string,score float64) int64{
	rank:=int64(0)
	node:=skiplist.header
	for i:=skiplist.level-1;i>=0;i--{
		if node.level[i]!=nil{
			//退出循环的时候，rank拿到的就是目标的前驱节点的排名
			for node.level[i].forward!=nil&&(node.level[i].forward.Score<score||
			node.level[i].forward.Score==score&&node.level[i].forward.Member<=member){
				rank+=node.level[i].span
				node=node.level[i].forward
			}
		}
	}
	//退出来检查，当前节点的member是否与传入的member一致
	if node!=nil&&node.Member==member{
		return rank
	}
	//如果没找到的话，返回0
	return 0
}

//根据排名获取到目标节点，从高层开始阶梯状往下走
func (skiplist *skiplist) getByRank(rank int64) *node{
	//拿哑节点，然后从当前最高层开始往下遍历，快速跳过一些不必要的节点
	node:=skiplist.header
	currentRank:=int64(0)
	for i:=skiplist.level-1;i>=0;i--{
		//每层初始的位置不为空，可以拿到层级信息
		if node.level[i]!=nil{
			//当前层次的下一个节点的排名都不够的话，才继续往下走，否则就往下沉
			for node.level[i].forward!=nil&&rank>=(currentRank+node.level[i].span){
				//拿到下一个节点的排名并往后移动
				currentRank+=node.level[i].span
				node=node.level[i].forward
			}
		}
	}
	//退出循环以后,其实就是第0层都遍历到下一个节点的排名值大于传入的rank了
	if currentRank==rank{
		return node
	}
	return nil
}

//移除排名范围为[start,stop)下的节点,返回移除的点的元素
func (skiplist *skiplist) RemoveRangeByRank(start int64, stop int64) (removed []*Element){
	var rank int64=0
	update:=make([]*node,maxLevel)
	removed=make([]*Element, 0)
	//从顶部开始扫描收集在范围内的节点
	node:=skiplist.header
	for i:=skiplist.level-1;i>=0;i--{
		if node.level[i]!=nil{
			//下一个节点的排名小于start，说明不在范围内，继续往右边走
			for node.level[i].forward!=nil&&(rank+node.level[i].span<=start){
				rank+=node.level[i].span
				node=node.level[i].forward
			}
		}
		update[i]=node
	}
	//退出循环以后，node节点的排名一定是等于start,删除到节点不为空，并且小于结束排名的所有节点
	for node!=nil&&rank<stop{
		//先拿下一个节点
		nextNode:=node.level[0].forward
		//收集删除的元素
		removed=append(removed,&node.Element)
		skiplist.removeNode(node, update)
		node=nextNode
		//更新下一个节点排名
		rank++
	}
	return removed
}

//传入上下范围区间，查询该跳表是否包含该范围下的元素
func (skiplist *skiplist) hasInRange(min Border,max Border) bool{
	//如果传入的参数有问题，上下边界是相互包含的
	if min.isIntersected(max){
		return false
	}
	//如果最小的边界比跳表最大元素都要大
	n:=skiplist.tail
	//跳表为空，或者跳表的最大值都小于下界
	if n==nil||!min.less(&n.Element){
		return false
	}
	//如果最大的边界比跳表最小的元素都要小
	n=skiplist.header.level[0].forward
	//跳表为空，或者跳表最小元素都大于上界
	if n==nil||!max.greater(&n.Element){
		return false
	}
	return true
}

//传入上下界，找到跳表内位于该区间范围的第一个元素
func (skiplist *skiplist) getFirstInRange(min Border,max Border) *node{
	//先检查跳表内是否在传入的区间范围有元素
	if !skiplist.hasInRange(min,max){
		//没有元素的话，那么直接返回空
		return nil
	}
	//拿到哑节点
	node:=skiplist.header
	//从上往下遍历
	for i:=skiplist.level-1;i>=0;i--{
		if node.level[i]!=nil{
			//从左往右遍历,如果下一个节点的值小于下界，需要往右边走
			for node.level[i].forward!=nil&&!min.less(&node.level[i].forward.Element){
				node=node.level[i].forward
			}
		}
	}
	//退出循环以后下面一个节点的值一定是大于等于下界的，往右移动一个
	node=node.level[0].forward
	//检查该值是否在上界以下
	if max.greater(&node.Element){
		return node
	}
	return nil
}

//传入上下界，找到跳表内该区间范围的最后一个元素
func (skiplist *skiplist) getLastInRange(min Border,max Border) *node{
	//先检查跳表内是否在传入的区间范围有元素
	if !skiplist.hasInRange(min,max){
		//没有元素的话，那么直接返回空
		return nil
	}
	node:=skiplist.header
	for i:=skiplist.level-1;i>=0;i--{
		if node.level[i]!=nil{
			//从左到右遍历的时候，下一个节点不为空，并且下一个节点值小于等于上界的时候，往右边走
			for node.level[i].forward!=nil&&max.greater(&node.level[i].forward.Element){
				node=node.level[i].forward
			}
		}
	}
	//退出来以后是位于第0层的最后一个小于等于上界的节点，检查一下是否大于等于下界
	if min.less(&node.Element){
		return node
	}
	return nil
}

//传入上下界,移除给定区间内的指定个节点，并返回移除元素的值
func (skiplist *skiplist) RemoveRange(min Border,max Border,limit int)(removed []*Element){
	//前驱管理切片与返回结果
	update:=make([]*node,maxLevel)
	removed=make([]*Element,0)
	node:=skiplist.header
	//从上往下扫描
	for i:=skiplist.level-1;i>=0;i--{
		if node.level[i]!=nil{
			for node.level[i].forward!=nil{
				if min.less(&node.level[i].forward.Element){
					break
				}
				node=node.level[i].forward
			}
		}
		update[i]=node
	}
	//往后面移动一个，就是第一个在范围内的元素了
	node=node.level[0].forward
	//开始删除节点，删除节点时，需要更改指针关系，以及上层索引
	for node!=nil{
		//超出给定范围了，需要退出循环
		if !max.greater(&node.Element){
			break
		}
		//传入节点地址与前驱节点切片，调用接口删除区间内节点
		removed=append(removed,&node.Element)
		//下一个节点地址
		nextNode:=node.level[0].forward
		skiplist.removeNode(node,update)
		//如果传入了限制个数，并且已经达到限制个数了，退出循环
		if limit>0&&len(removed)==limit{
			break
		}
		node=nextNode
	}
	return removed
}


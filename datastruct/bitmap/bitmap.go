package bitmap

/*
	采用字节切片模拟一个位图，支持动态扩容，扩容时传入当前总比特数目，通过比较当前位图是否能装下所有比特数决定是否扩容
	核心思想是，参数传入使用比特偏移量，底层实现是采用字节索引，拿到每个字节，然后通过位运算拿到具体的比特位置作特定操作
*/


type BitMap []byte

func New() *BitMap{
	b:=BitMap(make([]byte,0))
	return &b
}
//传入比特，拿到字节数，不足一字节按一字节算
func toByteSize(bitSize int64) int64{
	if bitSize%8==0{
		return bitSize/8
	}
	return bitSize/8+1
}
//传入位图中总比特数，拿到全部装下需要的字节数目，装不下时扩大相应字节数
func (b *BitMap) grow(bitSize int64){
	byteSize:=toByteSize(bitSize)
	gap:=byteSize-int64(len(*b))
	if gap<=0{
		return
	}
	*b=append(*b,make([]byte,gap)...)
}
//拿当前位图包含的比特位容量
func (b *BitMap) BitSize() int{
	return len(*b)*8
}
//字节切片转位图
func FromBytes(bytes []byte) *BitMap{
	b:=(BitMap(bytes))
	return &b
}
//位图转字节切片
func (b *BitMap) ToBytes() []byte{
	return *b
}
//设置位图中的某一位
func (b *BitMap) SetBit(offset int64, val byte){
	byteIndex:=offset/8
	byteOffset:=offset%8
	mask:=byte(1<<byteOffset)
	b.grow(offset+1)
	if val>0{
		(*b)[byteIndex]|=mask
	}else{
		(*b)[byteIndex]&=^mask
	}	
}
//拿到位图中的某一位
func (b *BitMap) GetBit(offset int64) byte{
	byteIndex:=offset/8
	byteOffset:=offset%8
	if byteIndex>=int64(len(*b)){
		return 0
	}
	return ((*b)[byteIndex]>>byteOffset)&0x01
}
//回调，由调用处实现函数体，对当前比特位作操作处理，不满足要求时返回false，满足时返回true
type Callback func(offset int64, val byte) bool
//遍历位图中的每一位
func (b *BitMap)ForEachBit(begin,end int64, cb Callback){
	offset:=begin
	byteIndex:=offset/8
	bitOffset:=offset%8
	for byteIndex<int64(len(*b)){
		b:=(*b)[byteIndex]
		for bitOffset<8{
			bit:=(b>>bitOffset)&0x01
			if !cb(offset,bit){
				return
			}
			offset++
			bitOffset++
			//end传0时表示需要遍历到末尾
			if offset>=end && end>0{
				break
			}
		}
		bitOffset=0
		byteIndex++
		if offset>=end && end>0{
			break
		}
	}
}
//遍历位图中的每一个字节
func (b *BitMap)ForEachByte(begin,end int, cb Callback){
	//0表示遍历到末尾
	if end==0{
		end=len(*b)
	}else if end>=len(*b){
		end=len(*b)
	}	
	for i:=begin;i<end;i++{
		//传入当前遍历到的字节索引,和该字节的值
		if !cb(int64(i),(*b)[i]){
			return 
		}
	}
}
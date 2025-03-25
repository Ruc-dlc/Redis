package redis

// Reply 接口定义回复的序列化方法
type Reply interface {
	ToBytes() []byte
}

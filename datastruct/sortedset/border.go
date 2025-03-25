package sortedset

import (
	"errors"
	"strconv"
)

//范围查询工具，提供给跳表和有序集合使用，提供分数和成员为边界的范围
//使用的时候，先去检查边界是否是无穷，无穷字段为0时才启用普通值字段结合排除字段构成开闭区间
//边界类型可以方便的作为范围类查询结构的入参，方便扩展

//定义常量作为分数与成员的无穷标记
const (
	scoreNegativeInf int8 = -1
	scorePositiveInf int8 = 1
	lexNegativeInf   int8 = '-'
	lexPositiveInf   int8 = '+'
)

//边界接口，提供接口约束，实际实现类有ScoreBorder和LexBorder分别为分数和成员的边界
type Border interface {
	//上界，传入的值小于等于该值，那么返回true，否则false
	greater(element *Element) bool
	//下界，传入的值大于等于该值，那么返回true，否则false
	less(element *Element) bool
	//拿到边界对象存放的普通字段值
	getValue() interface{}
	//闭区间为false，开区间为true
	getExclude() bool
	//传入另外一个边界点，查看两个边界是否相交，相交那么为真，否则为假
	isIntersected(max Border) bool
}

//分数边界，Inf字段为-1表示负无穷，为1表示正无穷，Value字段为边界值，Exclude字段为是否开区间
type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

//上界成员方法，传入的值小于等于上界，返回true
func (border *ScoreBorder) greater(element *Element) bool {
	//拿传入分数
	value := element.Score
	//如果上界是负无穷，那么没有元素小于上界，返回false
	if border.Inf == scoreNegativeInf {
		return false
		//如果上界的是正无穷，所有元素均小于上界，返回true
	} else if border.Inf == scorePositiveInf {
		return true
	}
	//检查开闭区间，开区间情况下，传入值必须严格小于边界值
	if border.Exclude {
		return border.Value > value
	}
	//闭区间，传入值小于等于上界，返回true
	return border.Value >= value
}

//下界成员方法，传入的值大于等于下界，返回true
func (border *ScoreBorder) less(element *Element) bool {
	//拿传入分数
	value := element.Score
	//如果下界是负无穷，所有元素都大于下界，返回true
	if border.Inf == scoreNegativeInf {
		return true
		//如果下界是正无穷，没有元素大于下界，返回false
	} else if border.Inf == scorePositiveInf {
		return false
	}
	//开区间的话，传入值必须严格大于边界值
	if border.Exclude {
		return border.Value < value
	}
	//闭区间允许传入值大于等于下界
	return border.Value <= value
}

//拿边界的普通字段值
func (border *ScoreBorder) getValue() interface{} {
	return border.Value
}
//检查边界是否开区间，开区间返回true，闭区间返回false
func (border *ScoreBorder) getExclude() bool {
	return border.Exclude
}

//定义正负无穷分数边界变量，用于在解析函数redis客户端命令中快速返回
var scorePositiveInfBorder = &ScoreBorder{
	Inf: scorePositiveInf,
}

var scoreNegativeInfBorder = &ScoreBorder{
	Inf: scoreNegativeInf,
}

//将s字符串解析为边界变量来返回，s来自于redis的客户端命令参数
func ParseScoreBorder(s string) (Border, error) {
	//检查是否是正负无穷大
	if s == "inf" || s == "+inf" {
		return scorePositiveInfBorder, nil
	}
	if s == "-inf" {
		return scoreNegativeInfBorder, nil
	}
	//如果不是正负无穷，那么就是普通值，剩下任务就是解析开闭区间与普通值了
	if s[0] == '(' {
		//开区间，解析到普通值
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("ERR min or max is not a float")
		}
		//解析没有出错的话，Inf字段赋为0，表示启用普通值，同时将排除字段设置为true
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: true,
		}, nil
	}
	//闭区间
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errors.New("ERR min or max is not a float")
	}
	return &ScoreBorder{
		Inf:     0,
		Value:   value,
		Exclude: false,
	}, nil
}
//在普通值的情况下，检查上下两个边界普通值是否交叉，交叉其实就是非法了
//换句话说，交叉就是没有元素，没交叉就是还有元素
func (border *ScoreBorder) isIntersected(max Border) bool {
	//分别拿到上下界普通值
	minValue := border.Value
	maxValue := max.(*ScoreBorder).Value
	//下界大于上界，或者下界等于上界但是开区间，那么两个边界交叉
	return minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}

//成员边界，用于有序集合的成员范围查询
type LexBorder struct {
	//无穷大字段，0表示正常值，-1表示负无穷，1表示正无穷
	Inf int8
	//普通值字段，当Inf字段为0时，该字段才有效,结合排除字段构成开闭区间
	Value string
	Exclude bool
}

//上界成员函数，当传入的值小于等于上界时返回true
func (border *LexBorder) greater(element *Element) bool {
	value := element.Member
	//如果上界是负无穷，返回假
	if border.Inf == lexNegativeInf {
		return false
		//如果上界是正无穷，返回真
	} else if border.Inf == lexPositiveInf {
		return true
	}
	//如果上界是开区间，那么传入值必须严格小于上界
	if border.Exclude {
		return border.Value > value
	}
	//闭区间，传入值允许小于等于上界
	return border.Value >= value
}

//接收者是下界，当传入的值大于等于时返回true
func (border *LexBorder) less(element *Element) bool {
	value := element.Member
	//如果边界是负无穷，所有值都大于等于，返回true
	if border.Inf == lexNegativeInf {
		return true
		//如果边界值是正无穷，没有值大于等于，返回false
	} else if border.Inf == lexPositiveInf {
		return false
	}
	//如果边界是开区间，那么传入的值需要严格大于边界值
	if border.Exclude {
		return border.Value < value
	}
	//闭区间，查看传入值是否大于等于下界
	return border.Value <= value
}
//拿成员值，仅在非无穷情形下启用
func (border *LexBorder) getValue() interface{} {
	return border.Value
}
//检查成员边界是否开闭区间，仅在非无穷情形下启用
func (border *LexBorder) getExclude() bool {
	return border.Exclude
}
//定义成员的正负无穷变量，便于在解析redis客户端请求快速返回
var lexPositiveInfBorder = &LexBorder{
	Inf: lexPositiveInf,
}

var lexNegativeInfBorder = &LexBorder{
	Inf: lexNegativeInf,
}

//将redis的命令解析为border变量
func ParseLexBorder(s string) (Border, error) {
	//先处理是否是正无穷
	if s == "+" {
		return lexPositiveInfBorder, nil
	}
	if s == "-" {
		return lexNegativeInfBorder, nil
	}
	//处理开闭区间
	if s[0] == '(' {
		//开区间的话
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: true,
		}, nil
	}

	if s[0] == '[' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: false,
		}, nil
	}
	return nil, errors.New("ERR min or max not valid string range item")
}
//如果两个边界有交集，或者说是否交叉，有元素，那么返回true
func (border *LexBorder) isIntersected(max Border) bool {
	//拿上下界普通值
	minValue := border.Value
	maxValue := max.(*LexBorder).Value
	//下界就是无穷大，下界大于上界，或者下界等于上界但是有一个是开区间，那么两个边界交叉，没有元素
	return border.Inf == '+' || minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}
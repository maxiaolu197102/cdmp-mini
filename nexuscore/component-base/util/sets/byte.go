/*
包摘要
核心类型:
Byte: 基于 map[byte]Empty 实现的 byte 类型的集合。Empty 是一个空结构体（struct{}），用作 map 的值，因为它不占用任何内存空间。

核心功能分类:

功能类别	方法名	功能描述
集合创建	NewByte	从可变参数列表创建并初始化一个 Byte 集合。
ByteKeySet	从一个 map 的键（key）创建 Byte 集合。
元素操作	Insert	向集合中添加一个或多个元素。
Delete	从集合中删除一个或多个元素。
PopAny	弹出并返回集合中的任意一个元素。
元素查询	Has	检查某个元素是否存在于集合中。
HasAll	检查是否所有给定元素都存在于集合中。
HasAny	检查是否存在任何一个给定的元素存在于集合中。
Len	返回集合中元素的数量。
集合运算	Difference	返回当前集合与另一个集合的差集。
Union	返回当前集合与另一个集合的并集。
Intersection	返回当前集合与另一个集合的交集。
IsSuperset	判断当前集合是否是另一个集合的超集。
Equal	判断两个集合是否相等（元素完全相同）。
集合输出	List	将集合中的元素以排序后的切片形式返回。
UnsortedList	将集合中的元素以未排序的切片形式返回。
函数使用方式及示例
1. 创建集合
go
package main

import (

	"fmt"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/sets"

)

	func main() {
	    // 方法一：使用 NewByte 函数，传入初始元素
	    set1 := sets.NewByte('a', 'b', 'c', '1')
	    fmt.Println("Set1:", set1.List()) // 输出: Set1: [1 97 98 99] (ASCII 码值排序)

	    // 方法二：先声明，再使用 Insert 方法
	    var set2 sets.Byte = sets.Byte{}
	    set2.Insert('x', 'y', 'z')
	    fmt.Println("Set2:", set2.UnsortedList())

	    // 方法三：从 map 的键创建集合
	    myMap := map[byte]int{
	        'k': 1,
	        'e': 2,
	        'y': 3,
	    }
	    set3 := sets.ByteKeySet(myMap) // 获取 myMap 的所有键作为集合
	    fmt.Println("Set3 (Keys of map):", set3.List()) // 输出: Set3 (Keys of map): [101 107 121] (e, k, y 的 ASCII 码)
	}

2. 基本操作（增删查）
go

	func main() {
	    chars := sets.NewByte('a', 'b', 'c')

	    // 插入元素
	    chars.Insert('d', 'e')
	    fmt.Println("After Insert:", chars.List())

	    // 检查元素是否存在
	    fmt.Println("Has 'a'?", chars.Has('a')) // true
	    fmt.Println("Has 'z'?", chars.Has('z')) // false
	    fmt.Println("HasAll 'a', 'c', 'e'?", chars.HasAll('a', 'c', 'e')) // true
	    fmt.Println("HasAny 'x', 'y', 'a'?", chars.HasAny('x', 'y', 'a')) // true

	    // 删除元素
	    chars.Delete('b', 'd')
	    fmt.Println("After Delete:", chars.List())

	    // 获取集合大小
	    fmt.Println("Length:", chars.Len())

	    // 弹出任意一个元素
	    if elem, exists := chars.PopAny(); exists {
	        fmt.Printf("Popped: %c\n", elem)
	        fmt.Println("Remaining set:", chars.List())
	    }
	}

3. 集合运算
go

	func main() {
	    setA := sets.NewByte('a', 'b', 'c', 'd')
	    setB := sets.NewByte('c', 'd', 'e', 'f')

	    // 并集 Union: 所有在 setA 或 setB 中的元素
	    unionSet := setA.Union(setB)
	    fmt.Printf("Union: %c\n", unionSet.List()) // 输出: Union: [a b c d e f]

	    // 交集 Intersection: 同时存在于 setA 和 setB 中的元素
	    intersectionSet := setA.Intersection(setB)
	    fmt.Printf("Intersection: %c\n", intersectionSet.List()) // 输出: Intersection: [c d]

	    // 差集 Difference: 在 setA 中但不在 setB 中的元素
	    differenceSet := setA.Difference(setB)
	    fmt.Printf("Difference (A - B): %c\n", differenceSet.List()) // 输出: Difference (A - B): [a b]

	    // 超集判断
	    smallSet := sets.NewByte('a', 'b')
	    fmt.Println("Is setA a superset of smallSet?", setA.IsSuperset(smallSet)) // true

	    // 集合相等判断
	    setC := sets.NewByte('a', 'b', 'c')
	    setD := sets.NewByte('c', 'b', 'a') // 顺序不同
	    fmt.Println("Are setC and setD equal?", setC.Equal(setD)) // true (集合元素无序)
	}

4. 获取集合内容
go

	func main() {
	    randomSet := sets.NewByte('z', 'a', 'm', '1', '9')

	    // 获取未排序的切片
	    unsorted := randomSet.UnsortedList()
	    fmt.Println("Unsorted:", unsorted) // 顺序随机，例如: [122 97 109 49 57]

	    // 获取排序后的切片 (按 byte 的数值，即 ASCII 码排序)
	    sorted := randomSet.List()
	    fmt.Println("Sorted:", sorted) // 输出: Sorted: [49 57 97 109 122] (对应字符 '1', '9', 'a', 'm', 'z')
	    // 为了看得更清楚，可以打印字符
	    for _, b := range sorted {
	        fmt.Printf("%c ", b)
	    }
	    // 输出: 1 9 a m z
	}

关键特性与注意事项
内存高效: 使用 map[T]Empty 而非 map[T]bool 或切片来实现集合，Empty 结构体不占内存，使得内存消耗最小化。

无序性: 集合本身是无序的，List() 方法提供了排序后的视图，而 UnsortedList() 提供了更高效的随机顺序视图。

类型安全: 专用于 byte 类型，避免了使用 interface{} 带来的类型断言开销和潜在错误。

操作链: 许多方法（如 Insert, Delete）返回集合本身，允许进行链式调用，例如 set.Insert('x').Delete('y')。

ByteKeySet 的恐慌（Panic）: 如果传入 ByteKeySet 的参数不是 map 类型，或者其 key 不是 byte 类型，函数会引发 panic。使用时需确保传入正确的类型。

ASCII 码排序: List() 方法按 byte 的数值（ASCII 码）进行排序，这在处理可打印字符时可能不是直观的字母顺序（例如数字排在字母前面）。

如何使用
核心区别：Set vs. Slice
特性	sets.Byte (集合)	[]byte (切片/数组)
核心语义	唯一性 的容器	有序 的序列
核心保证	保证元素不重复	保证元素顺序和可重复
主要操作	检查存在性、集合运算（交并差）	按索引访问、迭代、排序、追加
查询效率	O(1) - 极快 (map 哈希查找)	O(n) - 慢 (需要遍历整个切片)
典型用途	黑名单、白名单、标签、去重、数学集合	文件内容、网络数据包、字符串、二进制流
为什么需要 sets.Byte？（解决了什么问题）
高效的存在性检查 (最主要原因)

[]byte: 要检查一个 byte 是否在切片中，必须遍历整个切片，时间复杂度为 O(n)。对于大切片，这非常慢。

sets.Byte: 基于 map，检查 set.Has('x') 的时间复杂度是 O(1)，速度极快，与集合大小无关。

自动去重

向 sets.Byte 中重复插入相同的值，集合内容不会改变。这对于“获取所有不重复的字符”这类需求是天然支持的，而使用 []byte 需要手动写代码去重。

方便的集合运算

sets.Byte 直接提供了 Union (并集)、Intersection (交集)、Difference (差集) 等高级操作。用 []byte 实现这些功能需要写非常复杂和低效的循环代码。

举例说明：不同的使用场景
场景 1：检查一个字符是否是元音字母
使用 []byte (效率低)

go
vowels := []byte{'a', 'e', 'i', 'o', 'u'}
myChar := byte('e')

found := false
for _, v := range vowels { // 必须遍历整个切片

	    if v == myChar {
	        found = true
	        break
	    }
	}

fmt.Println(found)
使用 sets.Byte (效率高)

go
vowelsSet := sets.NewByte('a', 'e', 'i', 'o', 'u')
myChar := byte('e')

found := vowelsSet.Has(myChar) // 一次哈希查找，瞬间完成
fmt.Println(found)
结论：在这种检查存在性的场景下，sets.Byte 远胜于 []byte。

场景 2：处理用户输入的命令（保证顺序）
使用 []byte (正确选择)

go
userInput := []byte("ls -la /tmp")
// 命令的顺序至关重要！'l','s',' ','-','l'... 这个顺序不能改变。
// 这里需要的是有序的字节序列，而不是集合。
processCommand(userInput)
使用 sets.Byte (完全错误)

go
// 错误！集合会丢失所有顺序信息，并且删除重复的字符和空格。
userInputSet := sets.NewByte([]byte("ls -la /tmp")...)
// userInputSet 的内容可能变成 {' ', '-', '/', 'a', 'l', 'm', 'p', 's', 't'}，这根本不是原来的命令！
结论：在处理需要保持顺序的数据时，[]byte 是唯一的选择，sets.Byte 完全不适用。

场景 3：找出两段数据中都出现的字节（交集）
使用 []byte (复杂且低效)

go

	func findCommonBytes(data1, data2 []byte) []byte {
	    var common []byte
	    // 双重循环，时间复杂度 O(n*m)
	    for _, b1 := range data1 {
	        for _, b2 := range data2 {
	            if b1 == b2 {
	                // 还要检查是否已经存在于 common 中，避免重复添加
	                alreadyExists := false
	                for _, c := range common {
	                    if c == b1 {
	                        alreadyExists = true
	                        break
	                    }
	                }
	                if !alreadyExists {
	                    common = append(common, b1)
	                }
	            }
	        }
	    }
	    return common
	}

使用 sets.Byte (简单且高效)

go

	func findCommonBytes(data1, data2 []byte) []byte {
	    set1 := sets.NewByte(data1...)
	    set2 := sets.NewByte(data2...)
	    commonSet := set1.Intersection(set2) // 一行代码完成！
	    return commonSet.List()
	}

结论：对于集合运算，sets.Byte 提供了简洁而高效的 API，而用 []byte 实现则非常笨拙。

总结
如果你的需求是...	应该选择...
存储有序的字节序列（如文件内容、消息）	[]byte
快速检查某个字节是否存在（如校验、过滤）	sets.Byte
需要保证元素的唯一性（如获取所有出现的字符）	sets.Byte
进行集合运算，如求交集、并集、差集	sets.Byte
按索引访问或修改元素	[]byte
所以，sets.Byte 不是 []byte 的替代品，而是一个用于解决不同类型问题（集合问题）的专用工具。它在特定的场景下（存在性检查、去重、集合运算）能提供远超 []byte 的效率和代码简洁性。
*/
package sets

import (
	"reflect"
	"sort"
)

// sets.Byte is a set of bytes, implemented via map[byte]struct{} for minimal memory consumption.
type Byte map[byte]Empty

// NewByte creates a Byte from a list of values.
func NewByte(items ...byte) Byte {
	ss := Byte{}
	ss.Insert(items...)
	return ss
}

// ByteKeySet creates a Byte from a keys of a map[byte](? extends interface{}).
// If the value passed in is not actually a map, this will panic.
func ByteKeySet(theMap interface{}) Byte {
	v := reflect.ValueOf(theMap)
	ret := Byte{}

	for _, keyValue := range v.MapKeys() {
		ret.Insert(keyValue.Interface().(byte))
	}
	return ret
}

// Insert adds items to the set.
func (s Byte) Insert(items ...byte) Byte {
	for _, item := range items {
		s[item] = Empty{}
	}
	return s
}

// Delete removes all items from the set.
func (s Byte) Delete(items ...byte) Byte {
	for _, item := range items {
		delete(s, item)
	}
	return s
}

// Has returns true if and only if item is contained in the set.
func (s Byte) Has(item byte) bool {
	_, contained := s[item]
	return contained
}

// HasAll returns true if and only if all items are contained in the set.
func (s Byte) HasAll(items ...byte) bool {
	for _, item := range items {
		if !s.Has(item) {
			return false
		}
	}
	return true
}

// HasAny returns true if any items are contained in the set.
func (s Byte) HasAny(items ...byte) bool {
	for _, item := range items {
		if s.Has(item) {
			return true
		}
	}
	return false
}

// Difference returns a set of objects that are not in s2
// For example:
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
func (s Byte) Difference(s2 Byte) Byte {
	result := NewByte()
	for key := range s {
		if !s2.Has(key) {
			result.Insert(key)
		}
	}
	return result
}

// Union returns a new set which includes items in either s1 or s2.
// For example:
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
func (s1 Byte) Union(s2 Byte) Byte {
	result := NewByte()
	for key := range s1 {
		result.Insert(key)
	}
	for key := range s2 {
		result.Insert(key)
	}
	return result
}

// Intersection returns a new set which includes the item in BOTH s1 and s2
// For example:
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
func (s1 Byte) Intersection(s2 Byte) Byte {
	var walk, other Byte
	result := NewByte()
	if s1.Len() < s2.Len() {
		walk = s1
		other = s2
	} else {
		walk = s2
		other = s1
	}
	for key := range walk {
		if other.Has(key) {
			result.Insert(key)
		}
	}
	return result
}

// IsSuperset returns true if and only if s1 is a superset of s2.
func (s1 Byte) IsSuperset(s2 Byte) bool {
	for item := range s2 {
		if !s1.Has(item) {
			return false
		}
	}
	return true
}

// Equal returns true if and only if s1 is equal (as a set) to s2.
// Two sets are equal if their membership is identical.
// (In practice, this means same elements, order doesn't matter)
func (s1 Byte) Equal(s2 Byte) bool {
	return len(s1) == len(s2) && s1.IsSuperset(s2)
}

type sortableSliceOfByte []byte

func (s sortableSliceOfByte) Len() int           { return len(s) }
func (s sortableSliceOfByte) Less(i, j int) bool { return lessByte(s[i], s[j]) }
func (s sortableSliceOfByte) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// List returns the contents as a sorted byte slice.
func (s Byte) List() []byte {
	res := make(sortableSliceOfByte, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	sort.Sort(res)
	return []byte(res)
}

// UnsortedList returns the slice with contents in random order.
func (s Byte) UnsortedList() []byte {
	res := make([]byte, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	return res
}

// Returns a single element from the set.
func (s Byte) PopAny() (byte, bool) {
	for key := range s {
		s.Delete(key)
		return key, true
	}
	var zeroValue byte
	return zeroValue, false
}

// Len returns the size of the set.
func (s Byte) Len() int {
	return len(s)
}

func lessByte(lhs, rhs byte) bool {
	return lhs < rhs
}

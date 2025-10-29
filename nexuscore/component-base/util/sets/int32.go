/*
这个 sets.Int32 包提供了一个高效的 int32 集合实现，基于 map[int32]struct{} 实现，具有最小内存消耗。以下是详细的使用说明：

包摘要
sets.Int32 是一个类型安全的 int32 集合实现，提供了丰富的集合操作功能。

核心功能和使用方式
1. 创建集合
go
// 创建空集合
set1 := sets.NewInt32()

// 创建并初始化集合
set2 := sets.NewInt32(1, 2, 3, 4, 5)

// 从 map 的键创建集合
myMap := map[int32]string{1: "a", 2: "b", 3: "c"}
set3 := sets.Int32KeySet(myMap) // 包含 1, 2, 3
2. 基本操作
go
set := sets.NewInt32(1, 2, 3)

// 添加元素
set.Insert(4, 5)          // 现在包含 1, 2, 3, 4, 5

// 删除元素
set.Delete(1, 2)          // 现在包含 3, 4, 5

// 检查元素是否存在
exists := set.Has(3)      // true

// 检查所有元素是否存在
allExist := set.HasAll(3, 4) // true

// 检查任一元素是否存在
anyExist := set.HasAny(5, 6) // true (因为5存在)
3. 集合运算
go
setA := sets.NewInt32(1, 2, 3, 4)
setB := sets.NewInt32(3, 4, 5, 6)

// 差集：在A中但不在B中的元素
diff := setA.Difference(setB) // {1, 2}

// 并集：A和B的所有元素
union := setA.Union(setB)     // {1, 2, 3, 4, 5, 6}

// 交集：A和B的共同元素
intersection := setA.Intersection(setB) // {3, 4}

// 判断超集
isSuperset := setA.IsSuperset(sets.NewInt32(1, 2)) // true

// 判断集合相等
isEqual := setA.Equal(setB) // false
4. 遍历和转换
go
set := sets.NewInt32(3, 1, 4, 2)

// 获取有序列表（升序）
sortedList := set.List() // [1, 2, 3, 4]

// 获取无序列表
unsortedList := set.UnsortedList() // 顺序随机，如 [3, 1, 4, 2]

// 获取集合大小
size := set.Len() // 4

// 弹出任意一个元素
elem, exists := set.PopAny() // 返回一个元素和是否存在
业务场景
场景1：用户权限管理
go
// 用户拥有的权限
userPermissions := sets.NewInt32(1, 2, 3, 5)

// 操作需要的权限
requiredPermissions := sets.NewInt32(2, 3, 4)

// 检查用户是否有足够权限
if userPermissions.HasAll(requiredPermissions.List()...) {
    // 允许操作
} else {
    // 拒绝操作，可以找出缺少的权限
    missing := requiredPermissions.Difference(userPermissions)
    fmt.Println("缺少权限:", missing.List())
}
场景2：数据去重和过滤
go
// 从数据库查询可能有重复的数据
rawData := []int32{1, 2, 2, 3, 3, 3, 4, 5}

// 快速去重
uniqueSet := sets.NewInt32(rawData...)
uniqueData := uniqueSet.List() // [1, 2, 3, 4, 5]

// 过滤黑名单
blacklist := sets.NewInt32(2, 4)
filteredSet := uniqueSet.Difference(blacklist)
filteredData := filteredSet.List() // [1, 3, 5]
场景3：配置验证
go
// 有效的配置选项
validOptions := sets.NewInt32(1, 2, 3, 4, 5)

// 用户提供的配置
userOptions := sets.NewInt32(2, 3, 6, 7)

// 验证配置有效性
invalidOptions := userOptions.Difference(validOptions)
if invalidOptions.Len() > 0 {
    fmt.Println("无效的配置选项:", invalidOptions.List())
}
场景4：数据分析
go
// 两组数据的交集分析
groupA := sets.NewInt32(1, 2, 3, 4, 5)  // 用户组A的ID
groupB := sets.NewInt32(4, 5, 6, 7, 8)  // 用户组B的ID

// 找出同时在两个组的用户
commonUsers := groupA.Intersection(groupB) // {4, 5}

// 找出只在A组的用户
onlyInA := groupA.Difference(groupB) // {1, 2, 3}

// 所有不重复的用户
allUsers := groupA.Union(groupB) // {1, 2, 3, 4, 5, 6, 7, 8}
性能优势
O(1) 时间复杂度：查找、插入、删除操作都是常数时间

内存高效：使用 struct{} 作为值，不占用额外内存

类型安全：编译时类型检查，避免运行时错误

线程不安全：需要外部同步控制（如加锁）才能在并发场景使用

这个集合实现特别适合需要频繁进行集合运算、去重操作、成员检查等场景，比使用切片或数组的性能要好得多。

*/

package sets

import (
	"reflect"
	"sort"
)

// sets.Int32 is a set of int32s, implemented via map[int32]struct{} for minimal memory consumption.
type Int32 map[int32]Empty

// NewInt32 creates a Int32 from a list of values.
func NewInt32(items ...int32) Int32 {
	ss := Int32{}
	ss.Insert(items...)
	return ss
}

// Int32KeySet creates a Int32 from a keys of a map[int32](? extends interface{}).
// If the value passed in is not actually a map, this will panic.
func Int32KeySet(theMap interface{}) Int32 {
	v := reflect.ValueOf(theMap)
	ret := Int32{}

	for _, keyValue := range v.MapKeys() {
		ret.Insert(keyValue.Interface().(int32))
	}
	return ret
}

// Insert adds items to the set.
func (s Int32) Insert(items ...int32) Int32 {
	for _, item := range items {
		s[item] = Empty{}
	}
	return s
}

// Delete removes all items from the set.
func (s Int32) Delete(items ...int32) Int32 {
	for _, item := range items {
		delete(s, item)
	}
	return s
}

// Has returns true if and only if item is contained in the set.
func (s Int32) Has(item int32) bool {
	_, contained := s[item]
	return contained
}

// HasAll returns true if and only if all items are contained in the set.
func (s Int32) HasAll(items ...int32) bool {
	for _, item := range items {
		if !s.Has(item) {
			return false
		}
	}
	return true
}

// HasAny returns true if any items are contained in the set.
func (s Int32) HasAny(items ...int32) bool {
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
func (s Int32) Difference(s2 Int32) Int32 {
	result := NewInt32()
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
func (s1 Int32) Union(s2 Int32) Int32 {
	result := NewInt32()
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
func (s1 Int32) Intersection(s2 Int32) Int32 {
	var walk, other Int32
	result := NewInt32()
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
func (s1 Int32) IsSuperset(s2 Int32) bool {
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
func (s1 Int32) Equal(s2 Int32) bool {
	return len(s1) == len(s2) && s1.IsSuperset(s2)
}

type sortableSliceOfInt32 []int32

func (s sortableSliceOfInt32) Len() int           { return len(s) }
func (s sortableSliceOfInt32) Less(i, j int) bool { return lessInt32(s[i], s[j]) }
func (s sortableSliceOfInt32) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// List returns the contents as a sorted int32 slice.
func (s Int32) List() []int32 {
	res := make(sortableSliceOfInt32, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	sort.Sort(res)
	return []int32(res)
}

// UnsortedList returns the slice with contents in random order.
func (s Int32) UnsortedList() []int32 {
	res := make([]int32, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	return res
}

// Returns a single element from the set.
func (s Int32) PopAny() (int32, bool) {
	for key := range s {
		s.Delete(key)
		return key, true
	}
	var zeroValue int32
	return zeroValue, false
}

// Len returns the size of the set.
func (s Int32) Len() int {
	return len(s)
}

func lessInt32(lhs, rhs int32) bool {
	return lhs < rhs
}

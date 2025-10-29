/*
这个 sets.Int 包提供了一个基于 map[int]Empty 实现的整数（int）集合类型，旨在以最小内存消耗提供高效的集合操作。

包摘要
核心类型:

Int: 基于 map[int]Empty 实现的 int 类型的集合。

功能分类:

类别	方法	描述
集合创建	NewInt	从可变参数创建集合
IntKeySet	从 map 的键创建集合
元素操作	Insert	添加元素
Delete	删除元素
PopAny	弹出任意元素
元素查询	Has	检查元素是否存在
HasAll	检查所有元素是否存在
HasAny	检查任一元素是否存在
Len	获取集合大小
集合运算	Difference	差集
Union	并集
Intersection	交集
IsSuperset	超集判断
Equal	集合相等判断
集合输出	List	返回排序后的切片
UnsortedList	返回未排序的切片
函数使用方式及示例
1. 创建集合
go
// 从值创建
set1 := sets.NewInt(1, 2, 3, 4, 5)
fmt.Println(set1.List()) // [1 2 3 4 5]

// 从 map 键创建
userMap := map[int]string{
    1001: "Alice",
    1002: "Bob",
    1003: "Charlie",
}
userIDs := sets.IntKeySet(userMap)
fmt.Println(userIDs.List()) // [1001 1002 1003]

// 空集合插入
set2 := sets.Int{}
set2.Insert(10, 20, 30)
2. 基本操作
go
numbers := sets.NewInt(1, 2, 3, 4, 5)

// 检查存在性
fmt.Println("Has 3:", numbers.Has(3)) // true
fmt.Println("Has 7:", numbers.Has(7)) // false
fmt.Println("HasAll 2,4:", numbers.HasAll(2, 4)) // true
fmt.Println("HasAny 6,7,3:", numbers.HasAny(6, 7, 3)) // true

// 增删操作
numbers.Insert(6, 7)
numbers.Delete(1, 2)
fmt.Println("After operations:", numbers.List()) // [3 4 5 6 7]

// 弹出元素
if elem, exists := numbers.PopAny(); exists {
    fmt.Printf("Popped: %d, Remaining: %v\n", elem, numbers.List())
}
3. 集合运算
go
setA := sets.NewInt(1, 2, 3, 4)
setB := sets.NewInt(3, 4, 5, 6)

// 并集
union := setA.Union(setB)
fmt.Println("Union:", union.List()) // [1 2 3 4 5 6]

// 交集
intersection := setA.Intersection(setB)
fmt.Println("Intersection:", intersection.List()) // [3 4]

// 差集
diff := setA.Difference(setB)
fmt.Println("A - B:", diff.List()) // [1 2]

// 超集和相等判断
subset := sets.NewInt(1, 2)
fmt.Println("Is superset:", setA.IsSuperset(subset)) // true
fmt.Println("Are equal:", setA.Equal(setB)) // false
4. 输出转换
go
dataSet := sets.NewInt(5, 3, 8, 1, 9)

// 排序输出
sorted := dataSet.List()
fmt.Println("Sorted:", sorted) // [1 3 5 8 9]

// 未排序输出（更快）
unsorted := dataSet.UnsortedList()
fmt.Println("Unsorted:", unsorted) // 顺序随机，如 [9 1 5 8 3]

// 获取大小
fmt.Println("Size:", dataSet.Len()) // 5
使用场景
1. 用户ID/资源ID管理
go
// 活跃用户ID集合
activeUsers := sets.NewInt(1001, 1002, 1005, 1008)

// 检查用户是否活跃
func IsUserActive(userID int) bool {
    return activeUsers.Has(userID)
}

// 批量检查用户状态
func GetActiveUsers(userIDs []int) []int {
    inputSet := sets.NewInt(userIDs...)
    return inputSet.Intersection(activeUsers).List()
}
2. 权限和访问控制
go
// 用户拥有的权限ID集合
userPermissions := sets.NewInt(1, 3, 5, 7)

// 检查是否拥有所有所需权限
requiredPerms := []int{1, 3}
if userPermissions.HasAll(requiredPerms...) {
    // 允许访问
}

// 检查是否有任一权限
anyPerms := []int{5, 9, 12}
if userPermissions.HasAny(anyPerms...) {
    // 部分访问
}
3. 数据分析和去重
go
// 处理可能有重复的数据
rawData := []int{1, 2, 2, 3, 4, 4, 4, 5}

// 自动去重
uniqueData := sets.NewInt(rawData...)
fmt.Println("Unique values:", uniqueData.List()) // [1 2 3 4 5]

// 找出重复值（通过差集）
allSet := sets.NewInt(rawData...)
duplicates := allSet.Difference(uniqueData) // 实际上这里应该用其他方法
4. 配置和白名单
go
// 允许的服务端口
allowedPorts := sets.NewInt(80, 443, 8080, 3000)

// 检查端口是否允许
func IsPortAllowed(port int) bool {
    return allowedPorts.Has(port)
}

// 动态更新允许的端口
func UpdateAllowedPorts(newPorts []int) {
    allowedPorts.Insert(newPorts...)
}
5. 游戏开发
go
// 玩家获得的成就ID
playerAchievements := sets.NewInt(101, 103, 107)

// 检查是否完成成就系列
seriesAchievements := sets.NewInt(101, 102, 103, 104)
completedSeries := playerAchievements.Intersection(seriesAchievements)
if completedSeries.Len() == seriesAchievements.Len() {
    fmt.Println("完成整个成就系列！")
}
6. 算法处理
go
// 找出两个切片的共同元素
func FindCommonElements(slice1, slice2 []int) []int {
    set1 := sets.NewInt(slice1...)
    set2 := sets.NewInt(slice2...)
    return set1.Intersection(set2).List()
}

// 检查切片是否包含重复元素
func HasDuplicates(slice []int) bool {
    return len(slice) != sets.NewInt(slice...).Len()
}
优势总结
性能卓越: Has() 操作是 O(1) 时间复杂度，远快于切片的 O(n) 遍历

内存高效: 使用 Empty 结构体，比 map[int]bool 更节省内存

自动去重: 天然保证元素唯一性

丰富操作: 提供完整的集合运算功能

类型安全: 专用于 int 类型，避免类型转换开销

注意事项
IntKeySet 如果传入非 map 类型会 panic

集合是无序的，需要有序输出时使用 List()

适合处理需要唯一性和快速查找的整数数据

不适合需要保持元素顺序的场景

这个 sets.Int 包非常适合处理各种需要集合操作的整数数据场景，特别是在需要高性能查找和集合运算的应用中。

*/

package sets

import (
	"reflect"
	"sort"
)

// sets.Int is a set of ints, implemented via map[int]struct{} for minimal memory consumption.
type Int map[int]Empty

// NewInt creates a Int from a list of values.
func NewInt(items ...int) Int {
	ss := Int{}
	ss.Insert(items...)
	return ss
}

// IntKeySet creates a Int from a keys of a map[int](? extends interface{}).
// If the value passed in is not actually a map, this will panic.
func IntKeySet(theMap interface{}) Int {
	v := reflect.ValueOf(theMap)
	ret := Int{}

	for _, keyValue := range v.MapKeys() {
		ret.Insert(keyValue.Interface().(int))
	}
	return ret
}

// Insert adds items to the set.
func (s Int) Insert(items ...int) Int {
	for _, item := range items {
		s[item] = Empty{}
	}
	return s
}

// Delete removes all items from the set.
func (s Int) Delete(items ...int) Int {
	for _, item := range items {
		delete(s, item)
	}
	return s
}

// Has returns true if and only if item is contained in the set.
func (s Int) Has(item int) bool {
	_, contained := s[item]
	return contained
}

// HasAll returns true if and only if all items are contained in the set.
func (s Int) HasAll(items ...int) bool {
	for _, item := range items {
		if !s.Has(item) {
			return false
		}
	}
	return true
}

// HasAny returns true if any items are contained in the set.
func (s Int) HasAny(items ...int) bool {
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
func (s Int) Difference(s2 Int) Int {
	result := NewInt()
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
func (s1 Int) Union(s2 Int) Int {
	result := NewInt()
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
func (s1 Int) Intersection(s2 Int) Int {
	var walk, other Int
	result := NewInt()
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
func (s1 Int) IsSuperset(s2 Int) bool {
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
func (s1 Int) Equal(s2 Int) bool {
	return len(s1) == len(s2) && s1.IsSuperset(s2)
}

type sortableSliceOfInt []int

func (s sortableSliceOfInt) Len() int           { return len(s) }
func (s sortableSliceOfInt) Less(i, j int) bool { return lessInt(s[i], s[j]) }
func (s sortableSliceOfInt) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// List returns the contents as a sorted int slice.
func (s Int) List() []int {
	res := make(sortableSliceOfInt, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	sort.Sort(res)
	return []int(res)
}

// UnsortedList returns the slice with contents in random order.
func (s Int) UnsortedList() []int {
	res := make([]int, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	return res
}

// Returns a single element from the set.
func (s Int) PopAny() (int, bool) {
	for key := range s {
		s.Delete(key)
		return key, true
	}
	var zeroValue int
	return zeroValue, false
}

// Len returns the size of the set.
func (s Int) Len() int {
	return len(s)
}

func lessInt(lhs, rhs int) bool {
	return lhs < rhs
}

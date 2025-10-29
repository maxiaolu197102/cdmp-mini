/*
这是一个 sets.Int64 包的实现，提供了高效的 int64 类型集合操作。以下是详细的包摘要和使用说明：

包摘要
sets.Int64 是一个基于 map[int64]struct{} 实现的高效 int64 集合，具有最小内存消耗和丰富的集合操作功能。

核心功能和使用方式
1. 创建集合
go
// 创建空集合
set1 := sets.NewInt64()

// 创建并初始化集合
set2 := sets.NewInt64(1001, 1002, 1003, 1004)

// 从 map 的键创建集合（常用于转换）
userMap := map[int64]string{1001: "Alice", 1002: "Bob", 1003: "Charlie"}
userIDs := sets.Int64KeySet(userMap) // 包含 1001, 1002, 1003
2. 基本操作
go
set := sets.NewInt64(1001, 1002, 1003)

// 添加元素
set.Insert(1004, 1005)          // 现在包含 1001-1005

// 删除元素
set.Delete(1001, 1002)          // 现在包含 1003, 1004, 1005

// 检查元素是否存在
exists := set.Has(1003)         // true

// 检查所有元素是否存在
allExist := set.HasAll(1003, 1004) // true

// 检查任一元素是否存在
anyExist := set.HasAny(1005, 1006) // true (因为1005存在)
3. 集合运算
go
setA := sets.NewInt64(1001, 1002, 1003, 1004)
setB := sets.NewInt64(1003, 1004, 1005, 1006)

// 差集：在A中但不在B中的元素
diff := setA.Difference(setB) // {1001, 1002}

// 并集：A和B的所有元素
union := setA.Union(setB)     // {1001, 1002, 1003, 1004, 1005, 1006}

// 交集：A和B的共同元素
intersection := setA.Intersection(setB) // {1003, 1004}

// 判断超集
isSuperset := setA.IsSuperset(sets.NewInt64(1001, 1002)) // true

// 判断集合相等
isEqual := setA.Equal(setB) // false
4. 遍历和转换
go
set := sets.NewInt64(3001, 1001, 2001, 4001)

// 获取有序列表（升序）
sortedList := set.List() // [1001, 2001, 3001, 4001]

// 获取无序列表（性能更好）
unsortedList := set.UnsortedList() // 顺序随机

// 获取集合大小
size := set.Len() // 4

// 弹出任意一个元素
elem, exists := set.PopAny() // 返回一个元素和是否存在
业务场景
场景1：用户ID管理和权限控制
go
// 系统所有有效用户ID
allUserIDs := sets.NewInt64(1001, 1002, 1003, 1004, 1005)

// 在线用户ID
onlineUsers := sets.NewInt64(1001, 1003, 1005)

// 离线用户
offlineUsers := allUserIDs.Difference(onlineUsers)
fmt.Println("离线用户:", offlineUsers.List()) // [1002, 1004]

// VIP用户ID
vipUsers := sets.NewInt64(1001, 1004)

// 在线的VIP用户
onlineVIPs := onlineUsers.Intersection(vipUsers)
fmt.Println("在线VIP:", onlineVIPs.List()) // [1001]
场景2：大数据分析和去重
go
// 从日志中提取的用户ID（可能有重复）
logUserIDs := []int64{1001, 1002, 1002, 1003, 1003, 1003, 1004}

// 快速去重统计
uniqueUsers := sets.NewInt64(logUserIDs...)
fmt.Printf("独立用户数: %d\n", uniqueUsers.Len()) // 4

// 按时间分段统计
morningUsers := sets.NewInt64(1001, 1002, 1003)
afternoonUsers := sets.NewInt64(1002, 1003, 1004)

// 全天活跃用户
dailyActiveUsers := morningUsers.Union(afternoonUsers)
fmt.Println("日活用户:", dailyActiveUsers.List()) // [1001, 1002, 1003, 1004]
场景3：配置管理和验证
go
// 系统支持的配置ID
supportedConfigs := sets.NewInt64(1, 2, 3, 4, 5)

// 用户请求的配置
requestedConfigs := sets.NewInt64(2, 3, 6, 7)

// 验证配置有效性
invalidConfigs := requestedConfigs.Difference(supportedConfigs)
if invalidConfigs.Len() > 0 {
    fmt.Println("无效配置ID:", invalidConfigs.List()) // [6, 7]
}

// 有效的配置
validConfigs := requestedConfigs.Intersection(supportedConfigs)
fmt.Println("有效配置:", validConfigs.List()) // [2, 3]
场景4：分布式系统节点管理
go
// 集群中所有节点ID
allNodes := sets.NewInt64(2001, 2002, 2003, 2004, 2005)

// 健康节点
healthyNodes := sets.NewInt64(2001, 2002, 2003)

// 故障节点
failedNodes := allNodes.Difference(healthyNodes)
fmt.Println("故障节点:", failedNodes.List()) // [2004, 2005]

// 需要迁移的任务（假设在故障节点上运行的任务）
tasksOnFailedNodes := sets.NewInt64(5001, 5002, 5003, 5004)

// 可用的任务（在健康节点上运行的任务）
availableTasks := tasksOnFailedNodes.Difference(getTasksOnNodes(failedNodes))
场景5：电商平台商品管理
go
// 用户购物车商品ID
cartItems := sets.NewInt64(3001, 3002, 3003, 3004)

// 用户收藏夹商品ID
favoriteItems := sets.NewInt64(3002, 3003, 3005)

// 在购物车但未收藏的商品
cartNotFavorite := cartItems.Difference(favoriteItems)
fmt.Println("可考虑收藏:", cartNotFavorite.List()) // [3001, 3004]

// 既在购物车又在收藏夹的商品
bothCartAndFavorite := cartItems.Intersection(favoriteItems)
fmt.Println("重点关注:", bothCartAndFavorite.List()) // [3002, 3003]
性能优势
O(1) 时间复杂度：查找、插入、删除操作

内存高效：使用空结构体 struct{} 作为值

类型安全：编译时类型检查

丰富的集合操作：并集、交集、差集等

自动优化：交集操作会自动选择较小的集合进行遍历

适用场景
用户/商品/资源ID管理

权限和访问控制

数据去重和统计

配置验证和管理

分布式系统状态管理

大数据分析和处理

这个实现特别适合处理大规模 int64 ID 的集合操作，在微服务架构和分布式系统中非常有用。




*/

package sets

import (
	"reflect"
	"sort"
)

// sets.Int64 is a set of int64s, implemented via map[int64]struct{} for minimal memory consumption.
type Int64 map[int64]Empty

// NewInt64 creates a Int64 from a list of values.
func NewInt64(items ...int64) Int64 {
	ss := Int64{}
	ss.Insert(items...)
	return ss
}

// Int64KeySet creates a Int64 from a keys of a map[int64](? extends interface{}).
// If the value passed in is not actually a map, this will panic.
func Int64KeySet(theMap interface{}) Int64 {
	v := reflect.ValueOf(theMap)
	ret := Int64{}

	for _, keyValue := range v.MapKeys() {
		ret.Insert(keyValue.Interface().(int64))
	}
	return ret
}

// Insert adds items to the set.
func (s Int64) Insert(items ...int64) Int64 {
	for _, item := range items {
		s[item] = Empty{}
	}
	return s
}

// Delete removes all items from the set.
func (s Int64) Delete(items ...int64) Int64 {
	for _, item := range items {
		delete(s, item)
	}
	return s
}

// Has returns true if and only if item is contained in the set.
func (s Int64) Has(item int64) bool {
	_, contained := s[item]
	return contained
}

// HasAll returns true if and only if all items are contained in the set.
func (s Int64) HasAll(items ...int64) bool {
	for _, item := range items {
		if !s.Has(item) {
			return false
		}
	}
	return true
}

// HasAny returns true if any items are contained in the set.
func (s Int64) HasAny(items ...int64) bool {
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
func (s Int64) Difference(s2 Int64) Int64 {
	result := NewInt64()
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
func (s1 Int64) Union(s2 Int64) Int64 {
	result := NewInt64()
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
func (s1 Int64) Intersection(s2 Int64) Int64 {
	var walk, other Int64
	result := NewInt64()
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
func (s1 Int64) IsSuperset(s2 Int64) bool {
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
func (s1 Int64) Equal(s2 Int64) bool {
	return len(s1) == len(s2) && s1.IsSuperset(s2)
}

type sortableSliceOfInt64 []int64

func (s sortableSliceOfInt64) Len() int           { return len(s) }
func (s sortableSliceOfInt64) Less(i, j int) bool { return lessInt64(s[i], s[j]) }
func (s sortableSliceOfInt64) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// List returns the contents as a sorted int64 slice.
func (s Int64) List() []int64 {
	res := make(sortableSliceOfInt64, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	sort.Sort(res)
	return []int64(res)
}

// UnsortedList returns the slice with contents in random order.
func (s Int64) UnsortedList() []int64 {
	res := make([]int64, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	return res
}

// Returns a single element from the set.
func (s Int64) PopAny() (int64, bool) {
	for key := range s {
		s.Delete(key)
		return key, true
	}
	var zeroValue int64
	return zeroValue, false
}

// Len returns the size of the set.
func (s Int64) Len() int {
	return len(s)
}

func lessInt64(lhs, rhs int64) bool {
	return lhs < rhs
}

/*
这是一个 sets.String 包的实现，提供了高效的字符串集合操作。以下是详细的包摘要和使用说明：

包摘要
sets.String 是一个基于 map[string]struct{} 实现的高效字符串集合，具有最小内存消耗和丰富的集合操作功能，特别适合处理字符串数据的去重和集合运算。

核心功能和使用方式
1. 创建集合
go
// 创建空集合
set1 := sets.NewString()

// 创建并初始化集合
set2 := sets.NewString("apple", "banana", "orange")

// 从 map 的键创建集合
configMap := map[string]interface{}{"host": "localhost", "port": 8080, "debug": true}
configKeys := sets.StringKeySet(configMap) // 包含 "host", "port", "debug"
2. 基本操作
go
set := sets.NewString("apple", "banana", "orange")

// 添加元素
set.Insert("grape", "melon")          // 现在包含 5 种水果

// 删除元素
set.Delete("apple", "banana")         // 现在包含 "orange", "grape", "melon"

// 检查元素是否存在
exists := set.Has("orange")           // true

// 检查所有元素是否存在
allExist := set.HasAll("orange", "grape") // true

// 检查任一元素是否存在
anyExist := set.HasAny("melon", "peach")  // true (因为"melon"存在)
3. 集合运算
go
setA := sets.NewString("a", "b", "c", "d")
setB := sets.NewString("c", "d", "e", "f")

// 差集：在A中但不在B中的元素
diff := setA.Difference(setB) // {"a", "b"}

// 并集：A和B的所有元素
union := setA.Union(setB)     // {"a", "b", "c", "d", "e", "f"}

// 交集：A和B的共同元素
intersection := setA.Intersection(setB) // {"c", "d"}

// 判断超集
isSuperset := setA.IsSuperset(sets.NewString("a", "b")) // true

// 判断集合相等
isEqual := setA.Equal(setB) // false
4. 遍历和转换
go
set := sets.NewString("zebra", "apple", "monkey", "banana")

// 获取有序列表（按字母顺序排序）
sortedList := set.List() // ["apple", "banana", "monkey", "zebra"]

// 获取无序列表（性能更好）
unsortedList := set.UnsortedList() // 顺序随机

// 获取集合大小
size := set.Len() // 4

// 弹出任意一个元素
elem, exists := set.PopAny() // 返回一个元素和是否存在
业务场景
场景1：标签和分类系统
go
// 文章标签管理
articleTags := sets.NewString("golang", "backend", "database", "performance")

// 用户感兴趣的标签
userInterests := sets.NewString("golang", "frontend", "ui/ux")

// 推荐相关文章（基于共同标签）
commonTags := articleTags.Intersection(userInterests)
if commonTags.Len() > 0 {
    fmt.Println("推荐原因：共同标签", commonTags.List()) // ["golang"]
}

// 用户可能感兴趣的新标签
suggestedTags := articleTags.Difference(userInterests) // ["backend", "database", "performance"]
场景2：权限和角色管理
go
// 用户拥有的权限
userPermissions := sets.NewString("read:posts", "write:posts", "delete:posts")

// 操作需要的权限
requiredPermissions := sets.NewString("write:posts", "moderate:comments")

// 权限检查
if userPermissions.HasAll(requiredPermissions.List()...) {
    // 允许操作
} else {
    missing := requiredPermissions.Difference(userPermissions)
    fmt.Println("缺少权限:", missing.List()) // ["moderate:comments"]
}
场景3：配置管理和验证
go
// 系统支持的配置项
supportedConfigs := sets.NewString("host", "port", "timeout", "retries")

// 用户提供的配置
userConfigs := sets.StringKeySet(map[string]interface{}{
    "host":    "localhost",
    "port":    8080,
    "unknown": "value",
    "timeout": 30,
})

// 验证配置有效性
invalidConfigs := userConfigs.Difference(supportedConfigs)
if invalidConfigs.Len() > 0 {
    fmt.Println("无效配置项:", invalidConfigs.List()) // ["unknown"]
}
场景4：数据清洗和去重
go
// 从多个来源收集的数据（可能有重复）
rawEmails := []string{
    "alice@example.com", "bob@example.com",
    "alice@example.com", "charlie@example.com",
    "bob@example.com",
}

// 快速去重
uniqueEmails := sets.NewString(rawEmails...)
fmt.Printf("唯一邮箱数量: %d\n", uniqueEmails.Len()) // 3

// 黑名单过滤
blacklist := sets.NewString("spam@example.com", "bob@example.com")
cleanEmails := uniqueEmails.Difference(blacklist)
fmt.Println("有效邮箱:", cleanEmails.List()) // ["alice@example.com", "charlie@example.com"]
场景5：API 路由和端点管理
go
// 已注册的API端点
registeredEndpoints := sets.NewString(
    "/api/v1/users",
    "/api/v1/posts",
    "/api/v1/comments",
    "/api/v2/users",
)

// 请求的端点
requestedEndpoint := "/api/v1/products"

// 端点存在性检查
if registeredEndpoints.Has(requestedEndpoint) {
    // 处理请求
} else {
    // 返回404
    fmt.Println("端点不存在")
}

// 按版本分组端点
v1Endpoints := sets.NewString()
for endpoint := range registeredEndpoints {
    if strings.HasPrefix(endpoint, "/api/v1/") {
        v1Endpoints.Insert(endpoint)
    }
}
场景6：内容过滤和审核
go
// 敏感词库
sensitiveWords := sets.NewString("badword1", "badword2", "spam", "fraud")

// 用户输入内容
userContent := "这是一段包含spam和正常内容的内容"

// 检查是否包含敏感词
words := extractWords(userContent) // 提取单词的函数
contentWords := sets.NewString(words...)

foundSensitiveWords := contentWords.Intersection(sensitiveWords)
if foundSensitiveWords.Len() > 0 {
    fmt.Println("发现敏感词:", foundSensitiveWords.List()) // ["spam"]
}
性能优势
O(1) 时间复杂度：字符串查找、插入、删除操作

内存高效：使用空结构体 struct{} 作为值

字符串优化：特别适合处理字符串集合操作

丰富的API：完整的集合运算功能

自动排序：提供有序列表输出

适用场景
标签和分类系统

权限管理和访问控制

配置验证和管理

数据清洗和去重

内容过滤和审核

API 路由管理

关键词匹配和过滤

这个实现特别适合处理字符串集合操作，在Web开发、API服务、数据处理等场景中非常有用。
NewString:从字符串列表创建新的String集合
StringKeySet:从map的键创建String集合，自动提取所有string类型的键
Insert:向集合中添加一个或多个字符串元素
Delete:从集合中删除一个或多个字符串元素
Has:检查集合中是否包含指定字符串元素
HasAll:检查集合是否包含所有指定的字符串元素
HasAny:检查集合是否包含任意一个指定的字符串元素
Difference:返回当前集合与另一个集合的差集（在当前集合但不在另一个集合中的元素）
Union:返回当前集合与另一个集合的并集（所有不重复的元素）
Intersection:返回当前集合与另一个集合的交集（两个集合共有的元素）
IsSuperset:检查当前集合是否是另一个集合的超集（包含另一个集合的所有元素）
Equal:检查两个集合是否相等（包含完全相同的元素）
List:返回集合中所有元素的排序后切片（按字母顺序）
UnsortedList:返回集合中所有元素的未排序切片
PopAny:从集合中随机取出并删除一个元素
Len:返回集合中元素的数量

*/

package sets

import (
	"reflect"
	"sort"
)

// String is a set of strings, implemented via map[string]struct{} for minimal memory consumption.
type String map[string]Empty

// NewString creates a String from a list of values.
func NewString(items ...string) String {
	ss := String{}
	ss.Insert(items...)
	return ss
}

// StringKeySet creates a String from a keys of a map[string](? extends interface{}).
// If the value passed in is not actually a map, this will panic.
func StringKeySet(theMap interface{}) String {
	v := reflect.ValueOf(theMap)
	ret := String{}

	for _, keyValue := range v.MapKeys() {
		ret.Insert(keyValue.Interface().(string))
	}
	return ret
}

// Insert adds items to the set.
func (s String) Insert(items ...string) String {
	for _, item := range items {
		s[item] = Empty{}
	}
	return s
}

// Delete removes all items from the set.
func (s String) Delete(items ...string) String {
	for _, item := range items {
		delete(s, item)
	}
	return s
}

// Has returns true if and only if item is contained in the set.
func (s String) Has(item string) bool {
	_, contained := s[item]
	return contained
}

// HasAll returns true if and only if all items are contained in the set.
func (s String) HasAll(items ...string) bool {
	for _, item := range items {
		if !s.Has(item) {
			return false
		}
	}
	return true
}

// HasAny returns true if any items are contained in the set.
func (s String) HasAny(items ...string) bool {
	for _, item := range items {
		if s.Has(item) {
			return true
		}
	}
	return false
}

// Difference returns a set of objects that are not in s2
// For example:
// s = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s.Difference(s2) = {a3}
// s2.Difference(s) = {a4, a5}.
func (s String) Difference(s2 String) String {
	result := NewString()
	for key := range s {
		if !s2.Has(key) {
			result.Insert(key)
		}
	}
	return result
}

// Union returns a new set which includes items in either s or s2.
// For example:
// s = {a1, a2}
// s2 = {a3, a4}
// s.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s) = {a1, a2, a3, a4}.
func (s String) Union(s2 String) String {
	result := NewString()
	for key := range s {
		result.Insert(key)
	}
	for key := range s2 {
		result.Insert(key)
	}
	return result
}

// Intersection returns a new set which includes the item in BOTH s and s2
// For example:
// s = {a1, a2}
// s2 = {a2, a3}
// s.Intersection(s2) = {a2}.
func (s String) Intersection(s2 String) String {
	var walk, other String
	result := NewString()
	if s.Len() < s2.Len() {
		walk = s
		other = s2
	} else {
		walk = s2
		other = s
	}
	for key := range walk {
		if other.Has(key) {
			result.Insert(key)
		}
	}
	return result
}

// IsSuperset returns true if and only if s is a superset of s2.
func (s String) IsSuperset(s2 String) bool {
	for item := range s2 {
		if !s.Has(item) {
			return false
		}
	}
	return true
}

// Equal returns true if and only if s is equal (as a set) to s2.
// Two sets are equal if their membership is identical.
// (In practice, this means same elements, order doesn't matter).
func (s String) Equal(s2 String) bool {
	return len(s) == len(s2) && s.IsSuperset(s2)
}

type sortableSliceOfString []string

func (s sortableSliceOfString) Len() int           { return len(s) }
func (s sortableSliceOfString) Less(i, j int) bool { return lessString(s[i], s[j]) }
func (s sortableSliceOfString) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// List returns the contents as a sorted string slice.
func (s String) List() []string {
	res := make(sortableSliceOfString, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	sort.Sort(res)
	return []string(res)
}

// UnsortedList returns the slice with contents in random order.
func (s String) UnsortedList() []string {
	res := make([]string, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	return res
}

// PopAny returns a single element from the set.
func (s String) PopAny() (string, bool) {
	for key := range s {
		s.Delete(key)
		return key, true
	}
	var zeroValue string
	return zeroValue, false
}

// Len returns the size of the set.
func (s String) Len() int {
	return len(s)
}

func lessString(lhs, rhs string) bool {
	return lhs < rhs
}

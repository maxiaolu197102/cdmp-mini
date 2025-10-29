/*
这是一个空结构体，它不包含任何字段，因此不占用任何内存空间（Go 语言对空结构体有特殊优化）。

设计目的:
它的唯一用途是作为 map[T]Empty 中的值类型，用来实现集合（Set） 数据结构。在 Go 语言中，map[T]bool 也可以实现集合，但使用 Empty 更加内存高效。

使用场景
1. 实现内存高效的集合（核心场景）

*/

package sets

// Empty is public since it is used by some internal API objects for conversions between external
// string arrays and internal sets, and conversion logic requires public types today.
type Empty struct{}

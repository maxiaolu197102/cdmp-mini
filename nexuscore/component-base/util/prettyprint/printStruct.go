package prettyprint

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"text/tabwriter"
)

// Print 一键打印结构体（包括嵌套结构体）
func Print(title string, data interface{}) error {
	result, err := PrintStruct(title, data)
	if err != nil {
		return err
	}

	// 直接输出到标准输出
	fmt.Print(result)
	return nil
}

// printStruct 核心打印逻辑
func PrintStruct(title string, data interface{}) (string, error) {
	if data == nil {
		return fmt.Sprintf("=== %s ===\n<nil>\n", title), nil
	}

	// 获取终端宽度
	cols := getTerminalWidth()
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)

	// 写入标题
	writeTitle(tw, title, cols)

	// 递归打印结构体
	if err := printRecursive(tw, data, "", 0); err != nil {
		return "", err
	}

	// 刷新输出
	if err := tw.Flush(); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// getFieldName 获取字段名
func getFieldName(field reflect.StructField) string {
	if jsonTag := field.Tag.Get("json"); jsonTag != "" {
		if name := strings.Split(jsonTag, ",")[0]; name != "" && name != "-" {
			return name
		}
	}
	if mapTag := field.Tag.Get("mapstructure"); mapTag != "" {
		return mapTag
	}
	return field.Name
}

// printRecursive 递归打印（支持指针类型）
func printRecursive(tw *tabwriter.Writer, data interface{}, prefix string, depth int) error {
	val := reflect.ValueOf(data)

	// 处理指针类型
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			fmt.Fprintf(tw, "%s<nil>\n", prefix)
			return nil
		}
		val = val.Elem()
	}

	// 如果不是结构体，直接打印值
	if val.Kind() != reflect.Struct {
		fmt.Fprintf(tw, "%s%v\n", prefix, formatValue(val))
		return nil
	}

	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		if !field.CanInterface() {
			continue
		}

		fieldName := getFieldName(fieldType)
		fullPrefix := prefix + fieldName + ":"

		if isNestedStruct(field) {
			fmt.Fprintf(tw, "%s\n", fullPrefix)
			if err := printRecursive(tw, field.Interface(), prefix+"  ", depth+1); err != nil {
				return err
			}
		} else {
			// 使用 formatValue 来处理指针类型
			fmt.Fprintf(tw, "%s\t%s\n", fullPrefix, formatValue(field))
		}
	}

	return nil
}

// formatValue 格式化值，处理指针类型
func formatValue(field reflect.Value) string {
	// 处理指针类型
	if field.Kind() == reflect.Ptr {
		if field.IsNil() {
			return "<nil>"
		}
		// 获取指针指向的值
		elem := field.Elem()
		return formatValue(elem)
	}

	// 处理基本类型
	switch field.Kind() {
	case reflect.String:
		return field.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", field.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", field.Uint())
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", field.Float())
	case reflect.Bool:
		return fmt.Sprintf("%t", field.Bool())
	case reflect.Slice, reflect.Array:
		return formatSlice(field)
	default:
		return fmt.Sprintf("%v", field.Interface())
	}
}

// formatSlice 格式化切片和数组
func formatSlice(slice reflect.Value) string {
	if slice.IsNil() {
		return "<nil>"
	}

	var elements []string
	for i := 0; i < slice.Len(); i++ {
		element := slice.Index(i)
		elements = append(elements, formatValue(element))
	}
	return "[" + strings.Join(elements, ", ") + "]"
}

// isNestedStruct 判断嵌套结构体（更新以处理指针）
func isNestedStruct(field reflect.Value) bool {
	kind := field.Kind()

	// 处理指针类型
	if kind == reflect.Ptr {
		if field.IsNil() {
			return false
		}
		field = field.Elem()
		kind = field.Kind()
	}

	// 如果是结构体，并且不是time.Time等内置类型
	if kind == reflect.Struct {
		// 排除一些常见的内置类型
		typeName := field.Type().String()
		if strings.HasPrefix(typeName, "time.") ||
			strings.HasPrefix(typeName, "json.") ||
			strings.HasPrefix(typeName, "fmt.") {
			return false
		}
		return true
	}

	return false
}

package common

import (
	"unsafe"
)

type Bit bool

const (
	Zero Bit = false
	One  Bit = true
)

type EventWrapperMeta struct {
	Offset    int
	EventType byte
}

func IntToBytes(i int) []byte {
	// 使用 unsafe 转换 int 到字节数组
	bytes := (*[unsafe.Sizeof(int(0))]byte)(unsafe.Pointer(&i))
	return bytes[:]
}

const (
	ColumnTypeNone byte = iota
	ColumnDod
	ColumnTypeZstd
	ColumnTypeRle
	ColumnTypeLz4
)

const (
	DataLenVariable = 0x00
	DataLenFixed    = 0x01
)

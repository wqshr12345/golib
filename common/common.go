package common

import (
	"encoding/binary"
	"fmt"
	"io"
)

type CompressInfo struct {
	PkgId          int
	CompressType   int
	DataLen        int //original data
	CompressTime   int64
	TranportTime   int64
	DecompressTime int64
	CompressRatio  float64
}

type ReportFunction func(CompressInfo)

const (
	CompressTypeNovalid = iota
	CompressTypeNone
	CompressTypeLz4
	CompressTypeSnappy
	CompressTypeZstd
	CompressTypeGzip
	CompressTypeBzip2
	CompressTypeFlate
	CompressTypeZlib
	CompressTypeLzw
	CompressTypeBrotli
	CompressTypeRtc
)

func GetCompressionType(compressTypeStr string) (uint8, error) {
	switch compressTypeStr {
	case "None":
		return CompressTypeNone, nil
	case "Lz4":
		return CompressTypeLz4, nil
	case "Snappy":
		return CompressTypeSnappy, nil
	case "Zstd":
		return CompressTypeZstd, nil
	case "Gzip":
		return CompressTypeGzip, nil
	case "Bzip2":
		return CompressTypeBzip2, nil
	case "Flate":
		return CompressTypeFlate, nil
	case "Zlib":
		return CompressTypeZlib, nil
	case "Lzw":
		return CompressTypeLzw, nil
	case "Brotli":
		return CompressTypeBrotli, nil
	case "Rtc":
		return CompressTypeRtc, nil
	default:
		return CompressTypeNovalid, fmt.Errorf("invalid compress type: %s", compressTypeStr)
	}
}

type WriteFlusher interface {
	Write(b []byte) (n int, err error)
	Flush() error
}

type Compressor interface {
	Compress([]byte) []byte
}
type Decompressor interface {
	Decompress([]byte, []byte) []byte
}

// Binlog中用户列的类型
const (
	MYSQL_TYPE_DECIMAL byte = iota
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT

	// mysql 5.6
	MYSQL_TYPE_TIMESTAMP2
	MYSQL_TYPE_DATETIME2
	MYSQL_TYPE_TIME2
)

const (
	MYSQL_TYPE_JSON byte = iota + 0xf5
	MYSQL_TYPE_NEWDECIMAL
	MYSQL_TYPE_ENUM
	MYSQL_TYPE_SET
	MYSQL_TYPE_TINY_BLOB
	MYSQL_TYPE_MEDIUM_BLOB
	MYSQL_TYPE_LONG_BLOB
	MYSQL_TYPE_BLOB
	MYSQL_TYPE_VAR_STRING
	MYSQL_TYPE_STRING
	MYSQL_TYPE_GEOMETRY
)

type CompressPercent struct {
	CompressType byte

	Percent float64
}

// 与TableMap相关的函数

func IsBitSet(bitmap []byte, i int) bool {
	return bitmap[i>>3]&(1<<(uint(i)&7)) > 0
}

func IsBitSetIncr(bitmap []byte, i *int) bool {
	v := IsBitSet(bitmap, *i)
	*i++
	return v
}

func LengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	if len(b) == 0 {
		return 0, true, 0
	}

	switch b[0] {
	// 251: NULL
	case 0xfb:
		return 0, true, 1

		// 252: value of following 2
	case 0xfc:
		return uint64(b[1]) | uint64(b[2])<<8, false, 3

		// 253: value of following 3
	case 0xfd:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, false, 4

		// 254: value of following 8
	case 0xfe:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
				uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
				uint64(b[7])<<48 | uint64(b[8])<<56,
			false, 9
	}

	// 0-250: value of first byte
	return uint64(b[0]), false, 1
}

// LengthEncodedString returns the string read as a bytes slice, whether the value is NULL,
// the number of bytes read and an error, in case the string is longer than
// the input slice
func LengthEncodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := LengthEncodedInt(b)
	if num < 1 {
		return b[n:n], isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n : n], false, n, nil
	}
	return nil, false, n, io.EOF
}

func BitmapByteSize(columnCount int) int {
	return (columnCount + 7) / 8
}

func FixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(i) * 8)
	}
	return num
}

// 作为Aggregator的成员变量，存储每个TableId对应的元信息
type TableInfo struct {
	TableName   []byte
	ColumnCount uint64
	ColumnTypes []byte
	ColumnMeta  []uint16

	// len = (ColumnCount + 7) / 8
	NullBitmap []byte
}

func (ti *TableInfo) decodeMeta(data []byte) error {
	pos := 0
	ti.ColumnMeta = make([]uint16, ti.ColumnCount)
	for i, t := range ti.ColumnTypes {
		switch t {
		case MYSQL_TYPE_STRING:
			var x = uint16(data[pos]) << 8 // real type
			x += uint16(data[pos+1])       // pack or field length
			ti.ColumnMeta[i] = x
			pos += 2
		case MYSQL_TYPE_NEWDECIMAL:
			var x = uint16(data[pos]) << 8 // precision
			x += uint16(data[pos+1])       // decimals
			ti.ColumnMeta[i] = x
			pos += 2
		case MYSQL_TYPE_VAR_STRING,
			MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT:
			ti.ColumnMeta[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case MYSQL_TYPE_BLOB,
			MYSQL_TYPE_DOUBLE,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_GEOMETRY,
			MYSQL_TYPE_JSON:
			ti.ColumnMeta[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_TIME2,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP2:
			ti.ColumnMeta[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_SET,
			MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB,
			MYSQL_TYPE_LONG_BLOB:
			panic("unsupport type in binlog")
		default:
			ti.ColumnMeta[i] = 0
		}
	}

	return nil
}

func NewTableInfo(src []byte) *TableInfo {
	tableInfo := &TableInfo{}
	pos := 0

	pos += 1 // skip 0x00

	tableLength := src[pos]
	pos++

	tableInfo.TableName = src[pos : pos+int(tableLength)]
	pos += int(tableLength)

	pos++ // skip 0x00

	var n int
	tableInfo.ColumnCount, _, n = LengthEncodedInt(src[pos:])
	pos += n

	tableInfo.ColumnTypes = src[pos : pos+int(tableInfo.ColumnCount)]
	pos += int(tableInfo.ColumnCount)

	var err error
	var metaData []byte

	if metaData, _, n, err = LengthEncodedString(src[pos:]); err != nil {
		panic(err)
	}

	if err = tableInfo.decodeMeta(metaData); err != nil {
		panic(err)
	}
	pos += n

	nullBitmapSize := BitmapByteSize(int(tableInfo.ColumnCount))
	if len(src[pos:]) < nullBitmapSize {
		panic("EOF")
	}

	tableInfo.NullBitmap = src[pos : pos+nullBitmapSize]

	pos += nullBitmapSize

	// pos和len(src)不相等，后面有一些optional meta不想解析了...
	// if pos != len(src) {
	// 	// panic("not match")
	// 	fmt.Println("not match")
	// }
	return tableInfo
}

// 返回 数据长度 && 包含数据的字节数组
func decodeValue(data []byte, tp byte, meta uint16, isPartial bool) (v []byte, n int) {
	var length = 0

	if tp == MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				tp = b0 | 0x30
			} else {
				length = int(meta & 0xFF)
				tp = b0
			}
		} else {
			length = int(meta)
		}
	}

	switch tp {
	case MYSQL_TYPE_NULL:
		return nil, 0
	case MYSQL_TYPE_LONG:
		n = 4
		v = data[:n]
	case MYSQL_TYPE_TINY:
		n = 1
		v = data[:n]
	case MYSQL_TYPE_SHORT:
		n = 2
		v = data[:n]
	case MYSQL_TYPE_INT24:
		n = 3
		v = data[:n]
	case MYSQL_TYPE_LONGLONG:
		n = 8
		v = data[:n]
	case MYSQL_TYPE_NEWDECIMAL:
		// TODO(wangqian): 先这样，暂时panic
		panic("not support type MYSQL_TYPE_NEWDECIMAL")
	case MYSQL_TYPE_FLOAT:
		n = 4
		v = data[:n]
	case MYSQL_TYPE_DOUBLE:
		n = 8
		v = data[:n]
	case MYSQL_TYPE_BIT:
		panic("not support type MYSQL_TYPE_BIT")
	case MYSQL_TYPE_TIMESTAMP:
		n = 4
		v = data[:n]
	case MYSQL_TYPE_TIMESTAMP2:
		n = int(4 + (meta+1)/2)
		v = data[:n]
	case MYSQL_TYPE_DATETIME:
		n = 8
		v = data[:n]
	case MYSQL_TYPE_DATETIME2:
		n = int(5 + (meta+1)/2)
		v = data[:n]
	case MYSQL_TYPE_TIME:
		n = 3
		v = data[:n]

	case MYSQL_TYPE_TIME2:
		panic("not support type MYSQL_TYPE_TIME2")
	case MYSQL_TYPE_DATE:
		n = 3
		v = data[:n]
	case MYSQL_TYPE_YEAR:
		n = 1
		v = data[:n]
	case MYSQL_TYPE_ENUM:
		l := meta & 0xFF
		switch l {
		case 1:
			n = 1
			v = data[:n]
		case 2:
			n = 2
			v = data[:n]
		default:
			panic("Unknown ENUM packlen")
		}
	case MYSQL_TYPE_SET:
		n = int(meta & 0xFF)
		v = data[:n]
	case MYSQL_TYPE_BLOB:
		var length int
		// NOTE(wangqian): 这里的v应该把前面的表示长度的字节算进去
		switch meta {
		case 1:
			length = int(data[0])
			v = data[:1+length]
			n = length + 1
		case 2:
			length = int(binary.LittleEndian.Uint16(data))
			v = data[:2+length]
			n = length + 2
		case 3:
			length = int(FixedLengthInt(data[0:3]))
			v = data[:3+length]
			n = length + 3
		case 4:
			length = int(binary.LittleEndian.Uint32(data))
			v = data[:4+length]
			n = length + 4
		default:
			panic("invalid blob packlen")
		}
	case MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING:
		length = int(meta)
		if length < 256 {
			length = int(data[0])
			n = length + 1
		} else {
			length = int(binary.LittleEndian.Uint16(data[0:]))
			n = length + 2
		}
		// Note(wangqian):这里的n包括了2字节的长度
		v = data[:n]
	case MYSQL_TYPE_STRING:
		panic("not support type MYSQL_TYPE_STRING")
	case MYSQL_TYPE_JSON:
		n = length + int(meta)
		v = data[:n]
	case MYSQL_TYPE_GEOMETRY:
		panic("not support type MYSQL_TYPE_GEOMETRY")
	default:
		panic("unsupport type in binlog")
	}

	return v, n
}

// 根据table的meta data，将data部分的数据解析为一列列的数据，并append到output中
func DecodeImage(data []byte, includedColumns []byte, nullColumns []byte, ti *TableInfo, output *[][]byte) int {

	pos := 0
	// row := make([][]byte, ti.ColumnCount)
	if int(ti.ColumnCount) != len(*output) {
		panic("len(row) != len(*output)")
	}
	nullBitmapIndex := 0

	for i := 0; i < int(ti.ColumnCount); i++ {

		isPartial := false

		// TODOIMP(wangqian): 如果是为了测试，插入时必须全部都是全列插入，否则如果有一些列不在IncludedColumns中
		if !IsBitSet(includedColumns, i) {
			// skips = append(skips, i)
			continue
		}

		if IsBitSetIncr(nullColumns, &nullBitmapIndex) {
			// TODOIMP 测试中暂时不插入空值，否则这里不好处理
			// (*output)[i] = append((*output)[i], nil)
			continue
		}

		var n int
		data, n := decodeValue(data[pos:], ti.ColumnTypes[i], ti.ColumnMeta[i], isPartial)
		(*output)[i] = append((*output)[i], data...)

		pos += n
	}
	return pos
}

// ---与监控、预测压缩算法相关的common---
const (
	// 1. Binlog Stream Header
	BodyLen = 0 + iota
	SeqNum
	Flag1

	// 2. Binlog Event Header
	Timestamp
	EventType
	ServerId
	EventLen
	NextPos
	Flag

	// 3. Binlog Event Data
	FormatDescriptionEventAll

	TransactionPayloadEventAll

	PreviousGTIDsEventAll

	AnonymousGTIDEventAll

	RotateEventAll

	QueryEventAll

	XidEventAll

	// TableMap相关
	TableId
	NoUsed
	DbNameLen
	DbName
	TableInfo2

	// WriteRows相关
	TableId2
	Reserved
	ExtraInfoLen
	ExtraInfo
	ColumnNum
	IncludedColumn
	NullColumn
	CheckSum

	// User Column
	Int
	Double
	Long
	String
	TimeStamp
	Tiny

	TotalColumnNums
)

// 每一列的压缩算法
const (
	INVALID_START byte = 150 + iota
	NOCOMPRESSION
	SNAPPY
	// GZIP
	LZ4
	ZSTD
	RLE
	DELTA
	INVALID_END
)

type OffAndLen struct {
	Offset int64
	Len    int64
}

type CompressStats struct {
	OriLen   int
	CmprLen  int
	CmprTime int64
}

type ColumnCmpr struct {
	Column byte // 代表列名/类型
	Cmpr   byte // 代表压缩算法
}

type CompressionInfo struct {
	CompressionGain      float64
	CompressionBandwidth float64 // 压缩带宽 bytes/s
}

type CompressionIntro struct {
	Point   ColumnCmpr
	ByteNum int64
}

// ---与divide相关的common---

type DataWithInfo struct {
	Data        []byte
	Ts          int64
	TotalEvents int64
}

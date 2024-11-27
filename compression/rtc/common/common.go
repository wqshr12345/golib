package common

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/compression/zstd"
)

var (
	ZstdCompressor                  = zstd.NewCompressor()
	ZstdDecompressor                = zstd.NewDecompressor()
	RtcPerfs                        = NewRtcPerf()
	Sync             bool           = true
	Perf             bool           = false
	Wait             sync.WaitGroup = sync.WaitGroup{}
	CompressTime     int64          = 0
)

type Bit bool

const (
	Zero Bit = false
	One  Bit = true
)

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

func GetCompressorByName(compressType byte) common.Compressor {
	switch compressType {
	case CompressTypeZstd:
		return ZstdCompressor
	case CompressTypeNone:
		// return NewNoneCompressor()
	case CompressTypeLz4:
		// return NewLz4Compressor()
	case CompressTypeSnappy:
		// return NewSnappyCompressor()
	case CompressTypeGzip:
		// return NewGzipCompressor()
	case CompressTypeBzip2:
		// return NewBzip2Compressor()
	case CompressTypeFlate:
		// return NewFlateCompressor()
	case CompressTypeZlib:
		// return NewZlibCompressor()
	case CompressTypeLzw:
		// return NewLzwCompressor()
	case CompressTypeBrotli:
		// return NewBrotliCompressor()
	case CompressTypeRtc:
		// return NewRtcCompressor()
	default:
		panic("invalid compress type")
	}
	return nil
}

func GetDecompressorByName(compressType byte) common.Decompressor {
	switch compressType {
	case CompressTypeZstd:
		return ZstdDecompressor
	case CompressTypeNone:
		// return NewNoneDecompressor()
	case CompressTypeLz4:
		// return NewLz4Decompressor()
	case CompressTypeSnappy:
		// return NewSnappyDecompressor()
	case CompressTypeGzip:
		// return NewGzipDecompressor()
	case CompressTypeBzip2:
		// return NewBzip2Decompressor()
	case CompressTypeFlate:
		// return NewFlateDecompressor()
	case CompressTypeZlib:
		// return NewZlibDecompressor()
	case CompressTypeLzw:
		// return NewLzwDecompressor()
	case CompressTypeBrotli:
		// return NewBrotliDecompressor()
	case CompressTypeRtc:
		// return NewRtcDecompressor()
	default:
		panic("invalid decompress type")
	}
	return nil
}

type EventWrapperMeta struct {
	Offset          int
	EventType       byte
	TableId         uint64 // 仅在EventType == WRITE_ROWS_EVENT等情况下有用
	IncludedColumns []byte // 仅在EventType == UPDATE_ROWS_EVENT等情况下有用
}

// TODO 这里的type是否可以去掉？现在每列已经没有了默认的压缩方法...
const (
	ColumnTypeNone byte = iota
	ColumnDod
	ColumnTypeZstd
	ColumnTypeRle
	ColumnTypeLz4
	ColumnTypeDelta
	ColumnTypeRows // 专门用来区分Rows这一列的压缩类型
	ColumnTypeVarlen
	ColumnTypeAll
)

const (
	DataLenVariable = 0x00
	DataLenFixed    = 0x01
)

type ColumnType int

const (
	FLOAT ColumnType = iota
	DOUBLE
	BLOB
	JSON
	GEOMETRY
	BIT
	VARCHAR
	NEWDECIMAL
	SET
	ENUM
	STRING
	TIME_V2
	DATETIME_V2
	TIMESTAMP_V2
)

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

func MysqlTypeToString(mysqlType byte) string {
	switch mysqlType {
	case MYSQL_TYPE_DECIMAL:
		return "MYSQL_TYPE_DECIMAL"
	case MYSQL_TYPE_TINY:
		return "MYSQL_TYPE_TINY"
	case MYSQL_TYPE_SHORT:
		return "MYSQL_TYPE_SHORT"
	case MYSQL_TYPE_LONG:
		return "MYSQL_TYPE_LONG"
	case MYSQL_TYPE_FLOAT:
		return "MYSQL_TYPE_FLOAT"
	case MYSQL_TYPE_DOUBLE:
		return "MYSQL_TYPE_DOUBLE"
	case MYSQL_TYPE_NULL:
		return "MYSQL_TYPE_NULL"
	case MYSQL_TYPE_TIMESTAMP:
		return "MYSQL_TYPE_TIMESTAMP"
	case MYSQL_TYPE_LONGLONG:
		return "MYSQL_TYPE_LONGLONG"
	case MYSQL_TYPE_INT24:
		return "MYSQL_TYPE_INT24"
	case MYSQL_TYPE_DATE:
		return "MYSQL_TYPE_DATE"
	case MYSQL_TYPE_TIME:
		return "MYSQL_TYPE_TIME"
	case MYSQL_TYPE_DATETIME:
		return "MYSQL_TYPE_DATETIME"
	case MYSQL_TYPE_YEAR:
		return "MYSQL_TYPE_YEAR"
	case MYSQL_TYPE_NEWDATE:
		return "MYSQL_TYPE_NEWDATE"
	case MYSQL_TYPE_VARCHAR:
		return "MYSQL_TYPE_VARCHAR"
	case MYSQL_TYPE_BIT:
		return "MYSQL_TYPE_BIT"
	case MYSQL_TYPE_JSON:
		return "MYSQL_TYPE_JSON"
	case MYSQL_TYPE_NEWDECIMAL:
		return "MYSQL_TYPE_NEWDECIMAL"
	case MYSQL_TYPE_ENUM:
		return "MYSQL_TYPE_ENUM"
	case MYSQL_TYPE_SET:
		return "MYSQL_TYPE_SET"
	case MYSQL_TYPE_TINY_BLOB:
		return "MYSQL_TYPE_TINY_BLOB"
	case MYSQL_TYPE_MEDIUM_BLOB:
		return "MYSQL_TYPE_MEDIUM_BLOB"
	case MYSQL_TYPE_LONG_BLOB:
		return "MYSQL_TYPE_LONG_BLOB"
	case MYSQL_TYPE_BLOB:
		return "MYSQL_TYPE_BLOB"
	case MYSQL_TYPE_VAR_STRING:
		return "MYSQL_TYPE_VAR_STRING"
	case MYSQL_TYPE_STRING:
		return "MYSQL_TYPE_STRING"
	case MYSQL_TYPE_GEOMETRY:
		return "MYSQL_TYPE_GEOMETRY"
	case MYSQL_TYPE_TIMESTAMP2:
		return "MYSQL_TYPE_TIMESTAMP2"
	case MYSQL_TYPE_DATETIME2:
		return "MYSQL_TYPE_DATETIME2"
	case MYSQL_TYPE_TIME2:
		return "MYSQL_TYPE_TIME2"
	default:
		return "UNKNOWN"
	}
}

const (
	TABLE_MAP_OPT_META_SIGNEDNESS byte = iota + 1
	TABLE_MAP_OPT_META_DEFAULT_CHARSET
	TABLE_MAP_OPT_META_COLUMN_CHARSET
	TABLE_MAP_OPT_META_COLUMN_NAME
	TABLE_MAP_OPT_META_SET_STR_VALUE
	TABLE_MAP_OPT_META_ENUM_STR_VALUE
	TABLE_MAP_OPT_META_GEOMETRY_TYPE
	TABLE_MAP_OPT_META_SIMPLE_PRIMARY_KEY
	TABLE_MAP_OPT_META_PRIMARY_KEY_WITH_PREFIX
	TABLE_MAP_OPT_META_ENUM_AND_SET_DEFAULT_CHARSET
	TABLE_MAP_OPT_META_ENUM_AND_SET_COLUMN_CHARSET
	TABLE_MAP_OPT_META_COLUMN_VISIBILITY
)

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

type TableInfo struct {
	TableName   []byte
	ColumnCount uint64
	ColumnTypes []byte
	ColumnMeta  []uint16

	// len = (ColumnCount + 7) / 8
	NullBitmap []byte
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

func FixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(i) * 8)
	}
	return num
}

type RtcPerf struct {
	// Deserialize Time
	DtTime map[string]int64
	// Compress Time
	CpTime map[string]int64
	// NoSyncFinalize Time
	NsfTime sync.Map
	// SyncFinalize Time
	SfTime map[string]int64
	// Finalize Time
	FTime map[string]int64
}

func NewRtcPerf() *RtcPerf {
	return &RtcPerf{
		DtTime:  make(map[string]int64),
		CpTime:  make(map[string]int64),
		NsfTime: sync.Map{},
		SfTime:  make(map[string]int64),
		FTime:   make(map[string]int64),
		// TODO 写完这个
	}
}

func (r *RtcPerf) AddDtTime(key string, value int64) {
	if value == 0 {
		return
	}
	r.DtTime[key] += value
}

func (r *RtcPerf) AddCpTime(key string, value int64) {
	if value == 0 {
		return
	}
	r.CpTime[key] += value
}

func (r *RtcPerf) AddNsfTime(key string, value int64) {
	if value == 0 {
		return
	}
	// 虽然针对一个key没有多线程风险，但还是要使用sync.Map......
	oldValue, ok := r.NsfTime.Load(key)
	if !ok {
		r.NsfTime.Store(key, value)
		return
	}
	// 基于CAS去实现atomic incr...
	for !r.NsfTime.CompareAndSwap(key, oldValue, oldValue.(int64)+value) {
		oldValue, _ = r.NsfTime.Load(key)
	}
}

func (r *RtcPerf) AddSfTime(key string, value int64) {
	if value == 0 {
		return
	}
	r.SfTime[key] += value
}

func (r *RtcPerf) AddFTime(key string, value int64) {
	if value == 0 {
		return
	}
	r.FTime[key] += value
}

func (r *RtcPerf) DumpPerf() {
	totalDtTime := int64(0)
	for k, v := range r.DtTime {
		totalDtTime += v
		println("Deserialize Time: ", k, " ", v)
	}
	println("Deserialize Time: ", "total", " ", totalDtTime)

	totalCpTime := int64(0)
	for _, v := range r.CpTime {
		totalCpTime += v
		// println("Compress Time: ", k, " ", v)
	}
	println("Compress Time: ", "total", " ", totalCpTime)

	totalNsfTime := int64(0)

	r.NsfTime.Range(func(key, value interface{}) bool {
		totalNsfTime += value.(int64)
		// println("NoSyncFinalize Time: ", key.(string), " ", value.(int64))
		return true
	})

	println("NoSyncFinalize Time: ", "total", " ", totalNsfTime)

	totalSfTime := int64(0)
	for _, v := range r.SfTime {
		totalSfTime += v
		// println("SyncFinalize Time: ", k, " ", v)
	}
	println("SyncFinalize Time: ", "total", " ", totalSfTime)

	totalFTime := int64(0)
	for _, v := range r.FTime {
		totalFTime += v
		// println("Finalize Time: ", k, " ", v)
	}
	println("Finalize Time: ", "total", " ", totalFTime)

}

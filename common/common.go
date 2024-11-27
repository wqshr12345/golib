package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
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

func GetCompressionTypeStr(compressType uint8) string {
	switch compressType {
	case LZ4:
		return "Lz4"
	case SNAPPY:
		return "Snappy"
	case ZSTD1:
		return "Zstd1"
	case ZSTD:
		return "Zstd"
	case ZSTD8:
		return "Zstd8"
	case ZSTD22:
		return "Zstd22"
	case GZIP:
		return "Gzip"
	case FLATE:
		return "Flate"
	case LZO:
		return "Lzo"
	case NOCOMPRESSION:
		return "Nocompression"
	default:
		panic("no cmpr type")
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

func BFixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(len(buf)-i-1) * 8)
	}
	return num
}

const TIMEF_OFS int64 = 0x800000000000
const TIMEF_INT_OFS int64 = 0x800000

func decodeTime2(data []byte, dec uint16) ([]byte, int, error) {
	// time  binary length
	n := int(3 + (dec+1)/2)

	tmp := int64(0)
	intPart := int64(0)
	frac := int64(0)
	switch dec {
	case 1, 2:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(data[3])
		if intPart < 0 && frac != 0 {
			/*
			   Negative values are stored with reverse fractional part order,
			   for binary sort compatibility.

			     Disk value  intpart frac   Time value   Memory value
			     800000.00    0      0      00:00:00.00  0000000000.000000
			     7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
			     7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
			     7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
			     7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
			     7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960

			     Formula to convert fractional part from disk format
			     (now stored in "frac" variable) to absolute value: "0x100 - frac".
			     To reconstruct in-memory value, we shift
			     to the next integer value and then substruct fractional part.
			*/
			intPart++     /* Shift to the next integer value */
			frac -= 0x100 /* -(0x100 - frac) */
		}
		tmp = intPart<<24 + frac*10000
	case 3, 4:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(binary.BigEndian.Uint16(data[3:5]))
		if intPart < 0 && frac != 0 {
			/*
			   Fix reverse fractional part order: "0x10000 - frac".
			   See comments for FSP=1 and FSP=2 above.
			*/
			intPart++       /* Shift to the next integer value */
			frac -= 0x10000 /* -(0x10000-frac) */
		}
		tmp = intPart<<24 + frac*100

	case 5, 6:
		tmp = int64(BFixedLengthInt(data[0:6])) - TIMEF_OFS
		return timeFormat(tmp, dec, n)
	default:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		tmp = intPart << 24
	}

	if intPart == 0 && frac == 0 {
		return []byte("00:00:00"), n, nil
	}

	return timeFormat(tmp, dec, n)
}
func timeFormat(tmp int64, dec uint16, n int) ([]byte, int, error) {
	hms := int64(0)
	sign := ""
	if tmp < 0 {
		tmp = -tmp
		sign = "-"
	}

	hms = tmp >> 24

	hour := (hms >> 12) % (1 << 10) /* 10 bits starting at 12th */
	minute := (hms >> 6) % (1 << 6) /* 6 bits starting at 6th   */
	second := hms % (1 << 6)        /* 6 bits starting at 0th   */
	secPart := tmp % (1 << 24)

	if secPart != 0 {
		s := fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, secPart)
		return []byte(s[0 : len(s)-(6-int(dec))]), n, nil
	}

	return []byte(fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second)), n, nil
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
		prec := uint8(meta >> 8)
		scale := uint8(meta & 0xFF)
		v, n = decodeDecimal(data, int(prec), int(scale), false)
		// panic("not support type MYSQL_TYPE_NEWDECIMAL")
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
		v, n, _ = decodeTime2(data, meta)

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

const digitsPerInteger int = 9

var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
var zeros = [digitsPerInteger]byte{48, 48, 48, 48, 48, 48, 48, 48, 48}

func decodeDecimalDecompressValue(compIndx int, data []byte, mask uint8) (size int, value uint32) {
	size = compressedBytes[compIndx]
	switch size {
	case 0:
	case 1:
		value = uint32(data[0] ^ mask)
	case 2:
		value = uint32(data[1]^mask) | uint32(data[0]^mask)<<8
	case 3:
		value = uint32(data[2]^mask) | uint32(data[1]^mask)<<8 | uint32(data[0]^mask)<<16
	case 4:
		value = uint32(data[3]^mask) | uint32(data[2]^mask)<<8 | uint32(data[1]^mask)<<16 | uint32(data[0]^mask)<<24
	}
	return
}
func decodeDecimal(data []byte, precision int, decimals int, useDecimal bool) ([]byte, int) {
	// see python mysql replication and https://github.com/jeremycole/mysql_binlog
	integral := precision - decimals
	uncompIntegral := integral / digitsPerInteger
	uncompFractional := decimals / digitsPerInteger
	compIntegral := integral - (uncompIntegral * digitsPerInteger)
	compFractional := decimals - (uncompFractional * digitsPerInteger)

	binSize := uncompIntegral*4 + compressedBytes[compIntegral] +
		uncompFractional*4 + compressedBytes[compFractional]

	buf := make([]byte, binSize)
	copy(buf, data[:binSize])

	// must copy the data for later change
	data = buf

	// Support negative
	// The sign is encoded in the high bit of the the byte
	// But this bit can also be used in the value
	value := uint32(data[0])
	var res strings.Builder
	res.Grow(precision + 2)
	var mask uint32 = 0
	if value&0x80 == 0 {
		mask = uint32((1 << 32) - 1)
		res.WriteString("-")
	}

	// clear sign
	data[0] ^= 0x80

	zeroLeading := true

	pos, value := decodeDecimalDecompressValue(compIntegral, data, uint8(mask))
	if value != 0 {
		zeroLeading = false
		res.WriteString(strconv.FormatUint(uint64(value), 10))
	}

	for i := 0; i < uncompIntegral; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		if zeroLeading {
			if value != 0 {
				zeroLeading = false
				res.WriteString(strconv.FormatUint(uint64(value), 10))
			}
		} else {
			toWrite := strconv.FormatUint(uint64(value), 10)
			res.Write(zeros[:digitsPerInteger-len(toWrite)])
			res.WriteString(toWrite)
		}
	}

	if zeroLeading {
		res.WriteString("0")
	}

	if pos < len(data) {
		res.WriteString(".")

		for i := 0; i < uncompFractional; i++ {
			value = binary.BigEndian.Uint32(data[pos:]) ^ mask
			pos += 4
			toWrite := strconv.FormatUint(uint64(value), 10)
			res.Write(zeros[:digitsPerInteger-len(toWrite)])
			res.WriteString(toWrite)
		}

		if size, value := decodeDecimalDecompressValue(compFractional, data[pos:], uint8(mask)); size > 0 {
			toWrite := strconv.FormatUint(uint64(value), 10)
			padding := compFractional - len(toWrite)
			if padding > 0 {
				res.Write(zeros[:padding])
			}
			res.WriteString(toWrite)
			pos += size
		}
	}

	// if useDecimal {
	// 	f, _ := decimal.NewFromString(res.String())
	// 	return []byte(f), pos
	// }

	return []byte(res.String()), pos
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
	ZSTD1
	ZSTD
	ZSTD8
	ZSTD22
	SNAPPY
	LZ4
	LZO
	GZIP
	FLATE
	INVALID_END
	// XZ
	// RLE
	// DELTA
)

type OffAndLen struct {
	Name   string
	Offset int64
	Len    int64
}

type CompressStats struct {
	OriLen   int
	CmprLen  int
	CmprTime int64
}

type ColumnCmpr struct {
	Column string // 代表列名/类型
	Cmpr   byte   // 代表压缩算法
}

type CompressionInfo struct {
	CompressionGain      float64
	CompressionBandwidth float64 // 压缩带宽 bytes/s
	Epoch                int64   // 只要gain和bw更新，就更新epoch
	// UseEpoch             int64
}

type CmprTypeData struct {
	CmprType byte
	ByteNum  int
}

type CompressionIntro struct {
	CmprType byte
	ByteNum  int64
}

// ---与divide相关的common---

type DataWithInfo struct {
	Data        []byte
	Ts          int64
	TotalEvents int64
}

func CompressAndPrintInfo(v reflect.Value, prefix string, compressor Compressor) {
	v = reflect.Indirect(v) // 获取指针指向的值，如果是指针类型
	if v.Kind() != reflect.Struct {
		return
	}

	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		// 如果是[]byte，打印名字和内容
		if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.Uint8 {
			originLen := len(field.Bytes())
			if originLen == 0 {
				return
			}
			// cmprData := compressor.Compress(field.Bytes())
			// cmprLen := len(cmprData)
			// if fieldType.Name != "BodyLens" {
			// continue
			// }
			fmt.Printf("Field: %s.%s\n", prefix, fieldType.Name) // , oriLen: %v, cmprLen: %v, cmprRatio: %v       , originLen, cmprLen, float64(cmprLen)/float64(originLen)
			// bufferSize := 1 * 1024
			bufferSizes := []int{1 * 1024, 5 * 1024, 10 * 1024, 50 * 1024, 100 * 1024, 500 * 1024, 1000 * 1024}
			for _, bufferSize := range bufferSizes {
				// for {
				// for i := 100 * 1024; i <= 10*1024*1024; i += 100 * 1024 {
				averageProcessor := NewAverageProcessor(bufferSize, 1)
				averageProcessor.AddData(field.Bytes(), compressor, nil, ZSTD) //TODO修改
				averageProcessor.ComputeMedianAndVariance()
				// if bufferSize < 20*1024 {
				// bufferSize += 1 * 1024
				// } else if bufferSize < 1*1024*1024 {
				// bufferSize += 100 * 1024
				// } else if bufferSize < 10*1024*1024 {
				// bufferSize += 1 * 1024 * 1024
				// } else {
				// 	break
				// }
			}

		}

		// 如果是结构体，递归查找
		if field.Kind() == reflect.Struct {
			CompressAndPrintInfo(field, prefix+"."+fieldType.Name, compressor)
		}
	}
}

// 计算均值
func computeMean(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// 计算方差
func computeVariance(values []float64, mean float64) float64 {
	variance := 0.0
	for _, v := range values {
		variance += math.Pow(v-mean, 2)
	}
	return variance / float64(len(values)-1)
}

func ComputeMedianAndVariance(data []byte, blockSize int, compressor Compressor) {
	blocks := make([][]byte, 0)
	for len(data) > 0 {
		chooseSize := min(blockSize, len(data))
		block := data[:chooseSize]
		blocks = append(blocks, block)
		data = data[chooseSize:]
	}

	ratios := make([]float64, 0)
	for _, block := range blocks {
		originalSize := float64(len(block))
		compressedSize := float64(len(compressor.Compress(block)))
		ratios = append(ratios, compressedSize/originalSize)
	}

	// 计算均值
	mean := computeMean(ratios)

	// 计算方差
	variance := computeVariance(ratios, mean)
	fmt.Printf(" BlockSize: %d", blockSize)
	fmt.Printf(" Mean: %f", mean)
	fmt.Printf(" Variance: %f\n", variance)
}

type AverageProcessor struct {
	ratios          []float64
	bandwidths      []float64
	dcmprbandwidths []float64
	totalSize       int
	totalTime       float64
	totalDcmprTime  float64
	blockSize       int
	maxratio        float64
	minratio        float64
	cmprThread      int
	totalCmprSize   float64
}

func NewAverageProcessor(blockSize int, cmprThread int) *AverageProcessor {
	return &AverageProcessor{
		ratios:          make([]float64, 0),
		bandwidths:      make([]float64, 0),
		dcmprbandwidths: make([]float64, 0),
		blockSize:       blockSize,
		cmprThread:      cmprThread,
	}
}

func (a *AverageProcessor) AddData(data []byte, compressor Compressor, decompressor Decompressor, cmprType byte) {
	blocks := make([][]byte, 0)
	a.totalSize += len(data)
	for len(data) > 0 {
		chooseSize := min(a.blockSize, len(data))
		block := data[:chooseSize]
		blocks = append(blocks, block)
		data = data[chooseSize:]
	}

	for _, block := range blocks {
		originalSize := float64(len(block))
		// if originalSize < float64(a.blockSize) {
		// continue
		// }
		startTimes := time.Now()
		cmprBlock := compressor.Compress(block)
		compressedSize := float64(len(cmprBlock))
		endTimes := time.Now()
		a.totalTime += endTimes.Sub(startTimes).Seconds()

		a.ratios = append(a.ratios, compressedSize/originalSize)
		// 转成Mbps
		a.bandwidths = append(a.bandwidths, float64(len(block))/endTimes.Sub(startTimes).Seconds()/(1024*1024))
		a.maxratio = max(a.maxratio, compressedSize/originalSize)
		if a.minratio == 0.0 {
			a.minratio = a.maxratio
		}
		a.minratio = min(a.minratio, compressedSize/originalSize)

		originFileData := make([]byte, 0)
		if cmprType == LZ4 {
			originFileData = make([]byte, len(block))
		}
		startTimes2 := time.Now()
		decompressor.Decompress(originFileData, cmprBlock)
		endTimes2 := time.Now()
		a.totalDcmprTime += endTimes2.Sub(startTimes2).Seconds()
		a.totalCmprSize += compressedSize
		a.dcmprbandwidths = append(a.dcmprbandwidths, float64(len(cmprBlock))/endTimes.Sub(startTimes).Seconds()/(1024*1024))

	}
}

func (a *AverageProcessor) MultiThread() {
	// 计算均值
	fmt.Printf("CmprThread:%d, Compression Bandwidth2: %.6f, Decompression Bandwidth2: %.6f MB/s\n", a.cmprThread, float64(a.totalCmprSize)/a.totalTime/(1024*1024)*float64(a.cmprThread), float64(a.totalCmprSize)/a.totalDcmprTime/(1024*1024)*float64(a.cmprThread))
}

func (a *AverageProcessor) ComputeMedianAndVariance() {
	// 计算均值
	bandwidthMean := computeMean(a.bandwidths)
	bandwidthVariance := computeVariance(a.bandwidths, bandwidthMean)
	mean := computeMean(a.ratios)

	// 计算方差
	variance := computeVariance(a.ratios, mean)
	fmt.Printf(" BlockSize: %d", a.blockSize)
	fmt.Printf(" Mean: %f", mean)
	fmt.Printf(" Max %f", a.maxratio)
	fmt.Printf(" Min %f", a.minratio)
	fmt.Printf(" Variance: %f", variance)
	// fmt.Printf(" Mean: %f", mean)
	// fmt.Printf(" Max %f", a.maxratio)
	// fmt.Printf(" Min %f", a.minratio)
	fmt.Printf(" BandwidthVariance: %f", bandwidthVariance)
	fmt.Printf(" Compression Bandwidth: %.6f MB/s", float64(a.totalSize)/a.totalTime/(1024*1024))
	fmt.Printf(" Decompression Bandwidth: %.6f MB/s", float64(a.totalCmprSize)/a.totalDcmprTime/(1024*1024))
	fmt.Printf(" Total Time: %.6f", a.totalTime)
	fmt.Printf(" Total Cmpr Size: %.6f\n", a.totalCmprSize)
}

package event

import (
	"encoding/binary"
	"fmt"

	"github.com/wqshr12345/golib/compression/rtc/common"
)

type EventData interface {
	Tags() string
}

type FormatDescriptionEventData struct {
	BinlogVersion []byte // 2 bytes
	ServerVersion []byte // 50 bytes
	CreateTime    []byte // 4 bytes
	HeaderLen     []byte // 1 byte
	All           []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e FormatDescriptionEventData) Tags() string {
	return "BinlogVersion,ServerVersion,CreateTime,HeaderLen"
}

type StopEventData struct {
	All []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e StopEventData) Tags() string {
	return "All"

}

type TransactionPayloadEventData struct {
	All []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e TransactionPayloadEventData) Tags() string {
	return "All"
}

type PreviousGTIDsEventData struct {
	Flags []byte // 1 byte
	GTID  []byte // n bytes
	All   []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e PreviousGTIDsEventData) Tags() string {
	return "Flags,GTID"
}

type AnonymousGTIDEventData struct {
	GTID []byte // n bytes
	All  []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e AnonymousGTIDEventData) Tags() string {
	return "GTID"
}

type RotateEventData struct {
	Position []byte // 8 bytes noused
	NextName []byte // n bytes noused
	All      []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e RotateEventData) Tags() string {
	return "Position,NextName"
}

type QueryEventData struct {
	ThreadId     []byte // 4 bytes
	ExecTime     []byte // 4 bytes
	DbNameLen    []byte // 1 byte
	ErrorCode    []byte // 2 bytes
	StatusVarLen []byte // 2 bytes
	StatusVar    []byte // StatusVarLen bytes
	DbName       []byte // DbNameLen bytes
	Sql          []byte // (EventLen - 18 - DbNameLen) bytes
	All          []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e QueryEventData) Tags() string {
	return "ThreadId,ExecTime,DbNameLen,ErrorCode,StatusVarLen,StatusVar,DbName,Sql"
}

type TableMapEventData struct {
	TableId   []byte // 6 bytes
	NoUsed    []byte // 2 bytes
	DbNameLen []byte // 1 byte
	DbName    []byte // DbNameLen bytes
	TableInfo []byte
	All       []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e TableMapEventData) Tags() string {
	return "TableId,NoUsed,DbNameLen,DbName"
}

type WriteRowsEventData struct {
	TableId         []byte // 6 bytes
	Reserved        []byte // 2 bytes
	ExtraInfoLen    []byte // 2 bytes
	ExtraInfo       []byte // ExtraInfoLen bytes
	ColumnNums      []byte // 1 byte
	IncludedColumns []byte // (column_nums + 7) / 8 bytes
	NullColumns     []byte // (column_nums + 7) / 8 bytes
	Rows            []byte // TODO 暂时用统一压缩...
	Rows2           [][]byte
	Checksum        []byte // 4 bytes
	All             []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e WriteRowsEventData) Tags() string {
	return "TableId,Reserved,ExtraInfoLen,ExtraInfo,ColumnNums,IncludedColumns,NullColumns,Rows"
}

func (e *WriteRowsEventData) DecodeImage(data []byte, ti *common.TableInfo) (int, error) {
	// Rows_log_event::print_verbose_one_row()

	pos := 0

	// var isPartialJsonUpdate bool

	// var partialBitmap []byte
	// if e.eventType == PARTIAL_UPDATE_ROWS_EVENT && rowImageType == EnumRowImageTypeUpdateAI {
	// 	binlogRowValueOptions, _, n := LengthEncodedInt(data[pos:]) // binlog_row_value_options
	// 	pos += n
	// 	isPartialJsonUpdate = EnumBinlogRowValueOptions(binlogRowValueOptions)&EnumBinlogRowValueOptionsPartialJsonUpdates != 0
	// 	if isPartialJsonUpdate {
	// 		byteCount := bitmapByteSize(int(e.Table.JsonColumnCount()))
	// 		partialBitmap = data[pos : pos+byteCount]
	// 		pos += byteCount
	// 	}
	// }

	// TODO(wangqian): 1.3 下午
	row := make([][]byte, ti.ColumnCount)
	// skips := make([]int, 0)

	// refer: https://github.com/alibaba/canal/blob/c3e38e50e269adafdd38a48c63a1740cde304c67/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogBuffer.java#L63
	// NOTE(wangqian): null bitmap在前面解析过了
	// count := 0
	// for i := 0; i < int(e.ColumnCount); i++ {
	// 	if common.IsBitSet(bitmap, i) {
	// 		count++
	// 	}
	// }
	// count = common.BitmapByteSize(count)

	// nullBitmap := data[pos : pos+count]
	// pos += count

	// partialBitmapIndex := 0
	nullBitmapIndex := 0

	for i := 0; i < int(ti.ColumnCount); i++ {
		/*
		   Note: need to read partial bit before reading cols_bitmap, since
		   the partial_bits bitmap has a bit for every JSON column
		   regardless of whether it is included in the bitmap or not.
		*/
		// isPartial := isPartialJsonUpdate &&
		// 	(rowImageType == EnumRowImageTypeUpdateAI) &&
		// 	(e.Table.ColumnType[i] == common.MYSQL_TYPE_JSON) &&
		// 	isBitSetIncr(partialBitmap, &partialBitmapIndex)
		isPartial := false

		// TODOIMP(wangqian): 如果是为了测试，插入时必须全部都是全列插入，否则如果有一些列不在IncludedColumns中，就会不好弄...
		if !common.IsBitSet(e.IncludedColumns, i) {
			// skips = append(skips, i)
			continue
		}

		if common.IsBitSetIncr(e.NullColumns, &nullBitmapIndex) {
			row[i] = nil
			continue
		}

		var n int
		var err error
		row[i], n, err = e.decodeValue(data[pos:], ti.ColumnTypes[i], ti.ColumnMeta[i], isPartial)

		if err != nil {
			return 0, err
		}
		pos += n
	}

	e.Rows2 = row
	// e.SkippedColumns = append(e.SkippedColumns, skips)
	return pos, nil
}

// TODO(wangqian) 1.3 下午
func (e *WriteRowsEventData) decodeValue(data []byte, tp byte, meta uint16, isPartial bool) (v []byte, n int, err error) {
	var length = 0

	if tp == common.MYSQL_TYPE_STRING {
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
	case common.MYSQL_TYPE_NULL:
		return nil, 0, nil
	case common.MYSQL_TYPE_LONG:
		n = 4
		v = data[:n]
		// v = ParseBinaryInt32(data)
	case common.MYSQL_TYPE_TINY:
		n = 1
		v = data[:n]
		// v = ParseBinaryInt8(data)
	case common.MYSQL_TYPE_SHORT:
		n = 2
		v = data[:n]
		// v = ParseBinaryInt16(data)
	case common.MYSQL_TYPE_INT24:
		n = 3
		v = data[:n]
		// v = ParseBinaryInt24(data)
	case common.MYSQL_TYPE_LONGLONG:
		n = 8
		v = data[:n]
		// v = ParseBinaryInt64(data)
	case common.MYSQL_TYPE_NEWDECIMAL:
		// TODO(wangqian): 先这样，暂时panic
		panic("not support type MYSQL_TYPE_NEWDECIMAL")
		// prec := uint8(meta >> 8)
		// scale := uint8(meta & 0xFF)
		// v, n, err = decodeDecimal(data, int(prec), int(scale), e.useDecimal)
	case common.MYSQL_TYPE_FLOAT:
		n = 4
		v = data[:n]
		// v = ParseBinaryFloat32(data)
	case common.MYSQL_TYPE_DOUBLE:
		n = 8
		v = data[:n]
		// v = ParseBinaryFloat64(data)
	case common.MYSQL_TYPE_BIT:
		panic("not support type MYSQL_TYPE_BIT")
		// nbits := ((meta >> 8) * 8) + (meta & 0xFF)
		// n = int(nbits+7) / 8

		// // use int64 for bit
		// v, err = decodeBit(data, int(nbits), n)
	case common.MYSQL_TYPE_TIMESTAMP:
		n = 4
		v = data[:n]
		// t := binary.LittleEndian.Uint32(data)
		// if t == 0 {
		// 	v = formatZeroTime(0, 0)
		// } else {
		// 	v = e.parseFracTime(fracTime{
		// 		Time:                    time.Unix(int64(t), 0),
		// 		Dec:                     0,
		// 		timestampStringLocation: e.timestampStringLocation,
		// 	})
		// }
	case common.MYSQL_TYPE_TIMESTAMP2:
		n = int(4 + (meta+1)/2)
		v = data[:n]
		// panic("not support type MYSQL_TYPE_TIMESTAMP2")
		// v, n, err = decodeTimestamp2(data, meta, e.timestampStringLocation)
		// v = e.parseFracTime(v)
	case common.MYSQL_TYPE_DATETIME:
		n = 8
		v = data[:n]
		// i64 := binary.LittleEndian.Uint64(data)
		// if i64 == 0 {
		// 	v = formatZeroTime(0, 0)
		// } else {
		// 	d := i64 / 1000000
		// 	t := i64 % 1000000
		// 	v = e.parseFracTime(fracTime{
		// 		Time: time.Date(
		// 			int(d/10000),
		// 			time.Month((d%10000)/100),
		// 			int(d%100),
		// 			int(t/10000),
		// 			int((t%10000)/100),
		// 			int(t%100),
		// 			0,
		// 			time.UTC,
		// 		),
		// 		Dec: 0,
		// 	})
		// }
	case common.MYSQL_TYPE_DATETIME2:
		n = int(5 + (meta+1)/2)
		v = data[:n]
		// panic("not support type MYSQL_TYPE_DATETIME2")
		// v, n, err = decodeDatetime2(data, meta)
		// v = e.parseFracTime(v)
	case common.MYSQL_TYPE_TIME:
		n = 3
		v = data[:n]
		// i32 := uint32(FixedLengthInt(data[0:3]))
		// if i32 == 0 {
		// 	v = "00:00:00"
		// } else {
		// 	v = fmt.Sprintf("%02d:%02d:%02d", i32/10000, (i32%10000)/100, i32%100)
		// }
	case common.MYSQL_TYPE_TIME2:
		panic("not support type MYSQL_TYPE_TIME2")
		// v, n, err = decodeTime2(data, meta)
	case common.MYSQL_TYPE_DATE:
		n = 3
		v = data[:n]
		// i32 := uint32(FixedLengthInt(data[0:3]))
		// if i32 == 0 {
		// 	v = "0000-00-00"
		// } else {
		// 	v = fmt.Sprintf("%04d-%02d-%02d", i32/(16*32), i32/32%16, i32%32)
		// }

	case common.MYSQL_TYPE_YEAR:
		n = 1
		v = data[:n]
		// year := int(data[0])
		// if year == 0 {
		// 	v = year
		// } else {
		// 	v = year + 1900
		// }
	case common.MYSQL_TYPE_ENUM:
		l := meta & 0xFF
		switch l {
		case 1:
			// v = int64(data[0])
			n = 1
			v = data[:n]
		case 2:
			// v = int64(binary.LittleEndian.Uint16(data))
			n = 2
			v = data[:n]
		default:
			panic("Unknown ENUM packlen")
			// err = fmt.Errorf("Unknown ENUM packlen=%d", l)
		}
	case common.MYSQL_TYPE_SET:
		n = int(meta & 0xFF)
		v = data[:n]
		// nbits := n * 8

		// v, err = littleDecodeBit(data, nbits, n)
	case common.MYSQL_TYPE_BLOB:
		var length int
		// NOTE(wangqian): 重大BUG! 这里的v应该把前面的表示长度的字节算进去!!!
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
			length = int(common.FixedLengthInt(data[0:3]))
			v = data[:3+length]
			n = length + 3
		case 4:
			length = int(binary.LittleEndian.Uint32(data))
			v = data[:4+length]
			n = length + 4
		default:
			panic("invalid blob packlen")
			// err = fmt.Errorf("invalid blob packlen = %d", meta)
		}
		// panic("not support type MYSQL_TYPE_BLOB")
		// v, n, err = decodeBlob(data, meta)
	case common.MYSQL_TYPE_VARCHAR,
		common.MYSQL_TYPE_VAR_STRING:
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
		// panic("not support type MYSQL_TYPE_VARCHAR")
		// v, n = decodeString(data, length)
	case common.MYSQL_TYPE_STRING:
		panic("not support type MYSQL_TYPE_STRING")
		// v, n = decodeString(data, length)
	case common.MYSQL_TYPE_JSON:
		// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/AbstractRowsEventDataDeserializer.java#L404
		// length = int(FixedLengthInt(data[0:meta]))
		n = length + int(meta)
		v = data[:n]
		/*
			See https://github.com/mysql/mysql-server/blob/7b6fb0753b428537410f5b1b8dc60e5ccabc9f70/sql-common/json_binary.cc#L1077

			   Each document should start with a one-byte type specifier, so an
			   empty document is invalid according to the format specification.
			   Empty documents may appear due to inserts using the IGNORE keyword
			   or with non-strict SQL mode, which will insert an empty string if
			   the value NULL is inserted into a NOT NULL column. We choose to
			   interpret empty values as the JSON null literal.

			   In our implementation (go-mysql) for backward compatibility we prefer return empty slice.
		*/
		// if length == 0 {
		// 	v = []byte{}
		// } else {
		// 	if isPartial {
		// 		var diff *JsonDiff
		// 		diff, err = e.decodeJsonPartialBinary(data[meta:n])
		// 		if err == nil {
		// 			v = diff
		// 		} else {
		// 			fmt.Printf("decodeJsonPartialBinary(%q) fail: %s\n", data[meta:n], err)
		// 		}
		// 	} else {
		// 		var d []byte
		// 		d, err = e.decodeJsonBinary(data[meta:n])
		// 		if err == nil {
		// 			v = hack.String(d)
		// 		}
		// 	}
		// }
	case common.MYSQL_TYPE_GEOMETRY:
		panic("not support type MYSQL_TYPE_GEOMETRY")
		// MySQL saves Geometry as Blob in binlog
		// Seem that the binary format is SRID (4 bytes) + WKB, outer can use
		// MySQL GeoFromWKB or others to create the geometry data.
		// Refer https://dev.mysql.com/doc/refman/5.7/en/gis-wkb-functions.html
		// I also find some go libs to handle WKB if possible
		// see https://github.com/twpayne/go-geom or https://github.com/paulmach/go.geo
		// v, n, err = decodeBlob(data, meta)
	default:
		err = fmt.Errorf("unsupport type %d in binlog and don't know how to handle", tp)
	}

	return v, n, err
}

type XidEventData struct {
	Xid []byte // 8 bytes
	All []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e XidEventData) Tags() string {
	return "Xid"
}

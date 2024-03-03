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
	All           []byte //Note (Wang Qian) :We will compress all data in the event.
}

func (e FormatDescriptionEventData) Tags() string {
	return "BinlogVersion,ServerVersion,CreateTime,HeaderLen"
}

type StopEventData struct {
	All []byte //Note(Wang Qian) :We will compress all data in the event.
}

func (e StopEventData) Tags() string {
	return "All"
}

type TransactionPayLoadEventData struct {
	All []byte //Note (Wang Qian) ;We will compress all data in the event.
}

func (e TransactionPayLoadEventData) Tags() string {
	return "All"
}

type PreviousCTIDsEventData struct {
	Flags []byte // 1 byte
	GTID  []byte // n bytes
	All   []byte //Note(Wang Qian) :We will compress all dat the event.
}

func (e PreviousCTIDsEventData) Tags() string {
	return "Flags, GTID"
}

type AnonymousGTIDEventData struct {
	GTID []byte // n bytes
	All  []byte //Note (Wang Qian) :We will compress all data in the event.
}

func (e AnonymousGTIDEventData) Tags() string {
	return "GTID"
}

type RotateEventData struct {
	Position []byte // 8 bytes noused
	NextName []byte // n bytes noused
	All      []byte //Note (Wang Qian):We will compress all data in the event.
}

func (e RotateEventData) Tags() string {
	return "Position ‚NextName"
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
	All          []byte //Note (Wang Qian);We will compress all data in the event.
}

func (e QueryEventData) Tags() string {
	return "ThreadId, ExecTime, DbNameLen, Errorcode, StatusVarLen, StatusVar, DbName, Sql"
}

type TableMapEventData struct {
	TableId   []byte // 6 bytes
	NoUsed    []byte // 2 bytes
	DoNameLen []byte // 1 byte
	DbName    []byte // DbNameLen bytes
	TableInfo []byte
	All       []byte //Note (Wang Qian) :We will compress all data in the event.
}

func (e TableMapEventData) Tags() string {
	return "TableId,NoUsed, DbNameLen, DbName"
}

type WriteRowsEventData struct {
	TableId         []byte // 6 bytes
	Reserved        []byte // 2 bytes
	ExtraInfoLen    []byte // 2 bytes
	ExtraInfo       []byte // ExtraInfoLen bytes
	ColumnNums      []byte // 1 byte
	IncludedColumns []byte // (column_nums + 7) / 8 bytes
	NullColumns     []byte // (column_nums + 7) / 8 bytes
	Rows            []byte // TODO 暂时用统一压缩
	Rows2           [][]byte
	Checksum        []byte // 4 bytes
	All             []byte //Note (Wang Qian) :We will compress all data in the event.
}

func (e WriteRowsEventData) Tags() string {
	return "TableId,Reserved,ExtraInfoLen,Extrainfo,ColumnNums,IncludedColumns,NullColumns,Rows"
}

func (e *WriteRowsEventData) DecodeImage(data []byte, ti *common.TableInfo) (int, error) {
	// Rows_log_event::print_verbose_one_row()
	pos := 0
	// var isPartialJsonUpdate bool

	// TODO (wangqian): 1.3 下午
	row := make([][]byte, ti.ColumnCount)
	// skips := make([]int,0)

	nullBitmapIndex := 0

	for i := 0; i < int(ti.ColumnCount); i++ {
		isPartial := false
		if !common.IsBitSet(e.IncludedColumns, i) {
			// skips = append(skips, 1)
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
	// e.SkippedColumns = append (e.SkippedColumns, skips)
	return pos, nil
}

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
		// v = ParseBinaryInt32 (data)
	case common.MYSQL_TYPE_TINY:
		n = 1
		v = data[:n]
		// v = ParseBinaryInt8 (data)
	case common.MYSQL_TYPE_SHORT:
		n = 2
		v = data[:n]
		// v = ParseBinaryInt16 (data)
	case common.MYSQL_TYPE_INT24:
		n = 3
		v = data[:n]
		// v = ParseBinaryInt24 (data)
	case common.MYSQL_TYPE_LONGLONG:
		n = 8
		v = data[:n]
		// y = ParseBinaryInt64 (data)
	case common.MYSQL_TYPE_DECIMAL:
		// TODO (wangqian) : 先这样，暂时panic
		panic("not support type MYSOL_TYPE_NEWDECIMAL")
		// prec := uint8(meta >> 8)
		// scale := uint8(meta & OxFF)
		// v, n, err = decodeDecimal(data, int (prec), int(scale), euseDecimal)
	case common.MYSQL_TYPE_FLOAT:
		n = 4
		v = data[:n]
		// v = ParseBinaryFloat32 (data)
	case common.MYSQL_TYPE_DOUBLE:
		n = 8
		v = data[:n]
		// v = ParseBinaryFloat64 (data)
	case common.MYSQL_TYPE_BIT:
		panic("not support type MYSQL_TYPE_BIT")
	case common.MYSQL_TYPE_TIMESTAMP:
		n = 4
		v = data[:n]
	case common.MYSQL_TYPE_TIMESTAMP2:
		n = int(4 + (meta+1)/2)
		v = data[:n]
	case common.MYSQL_TYPE_DATETIME:
		n = 8
		v = data[:n]
	case common.MYSQL_TYPE_DATETIME2:
		n = int(5 + (meta+1)/2)
		v = data[:n]
	case common.MYSQL_TYPE_TIME:
		n = 3
		v = data[:n]
	case common.MYSQL_TYPE_TIME2:
		panic("not support type MYSQL_TYPE_TIME2")
		// v, n, err = decodeTime2 (data, meta)
	case common.MYSQL_TYPE_DATE:
		n = 3
		v = data[:n]
	case common.MYSQL_TYPE_YEAR:
		n = 1
		v = data[:n]
	case common.MYSQL_TYPE_ENUM:
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
	case common.MYSQL_TYPE_SET:
		n = int(meta & 0xFF)
		v = data[:n]

	case common.MYSQL_TYPE_BLOB:
		var length int
		// NOTE (wangqian)：重大BUG！这里的v应该把前面的表示长度的字节算进去！
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
		}
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
		// Note (wangqian)：这里的n包括了2字节的长度
		v = data[:n]
		// panic("not support type MYSQL_TYPE_VARCHAR")
		// v, n = decodeString (data, length)
	case common.MYSQL_TYPE_STRING:
		panic("not support type MYSQL_TYPE_STRING")
	case common.MYSQL_TYPE_JSON:
		n = length + int(meta)
		v = data[:n]
	case common.MYSQL_TYPE_GEOMETRY:
		panic("not support type MYSQL_TYPE_GEOMETRY")
	default:
		err = fmt.Errorf("unsupport type %d in binlog and don't know how to handle", tp)
	}

	return v, n, err
}

type XidEventData struct {
	Xid []byte // 8 bytes
	All []byte //Note (Wang Qian) :We will compress all data in the event.
}

func (e XidEventData) Tags() string {
	return "Xid"
}

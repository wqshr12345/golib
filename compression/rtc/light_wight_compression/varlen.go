package lightwightcompression

import (
	"encoding/binary"
	"unsafe"

	"github.com/wqshr12345/golib/compression/rtc/common"
)

type VarLenCompressor struct {
	dataLenType byte // compressor接收到的每个src[]是定长还是变长
	datalen     int  // compressor接收到的每个src[]的固定长度，仅在datalenType为定长时有效

	buf        []byte // 存储压缩后数据...
	rowNumbers uint32

	name string
	perf *common.RtcPerf
}

// pending 还没决定用[]byte还是int作为入参...

func NewVarLenCompressor(dataLenType byte, datalen int, name string, perf *common.RtcPerf) *VarLenCompressor {
	return &VarLenCompressor{
		dataLenType: dataLenType,
		datalen:     datalen,
		buf:         make([]byte, 0),
		rowNumbers:  0,
		name:        name,
		perf:        perf,
	}
}

func (v *VarLenCompressor) Compress(src []byte) {
	if v.datalen == 4 {
		val := *(*int32)(unsafe.Pointer(&src[0]))
		for val >= 0x80 {
			v.buf = append(v.buf, byte(val)|0x80)
			val >>= 7
		}
	} else if v.datalen == 8 {
		val := *(*int64)(unsafe.Pointer(&src[0]))
		for val >= 0x80 {
			v.buf = append(v.buf, byte(val)|0x80)
			val >>= 7
		}
	}
	// v.buf = append(v.buf, src...)
	v.rowNumbers += 1
}

func (v *VarLenCompressor) Finalize(out *[]byte, offset int) int {
	// startTime := time.Now().UnixNano()
	// 1. column foramt.
	offset = v.finalizeColumnFormat(out, offset, v.dataLenType)
	// 2. compressed data.
	copy((*out)[offset:], v.buf)
	offset += len(v.buf)
	// fmt.Println("压缩类型: No", "压缩前总长度：", c.rowNumbers*uint32(c.datalen), "压缩后总长度：", len(c.buf))

	// c.perf.AddFTime(c.name, time.Now().UnixNano()-startTime)
	return offset
}

func (v *VarLenCompressor) finalizeColumnFormat(out *[]byte, offset int, datalenType byte) int {
	// 1. compression type.
	(*out)[offset] = common.ColumnTypeVarlen
	offset += 1

	// 2. meta data(标识一些特殊的event，需要做特殊处理) TODO(wangqian)

	// 2. compressed data len.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(v.buf)))
	offset += 4

	// 3. variable or fixed.
	(*out)[offset] = datalenType
	offset += 1
	// 4. row numbers.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], v.rowNumbers)
	offset += 4

	// 5. fixed data len or variable data lens.
	if datalenType == common.DataLenFixed {
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(v.datalen))
		offset += 4
	} else if datalenType == common.DataLenVariable {
		panic("VarLenCompressor not support variable data len.")
	}

	return offset
}

package lightwightcompression

import (
	"encoding/binary"
	"reflect"
	"time"

	"github.com/wqshr12345/golib/compression/rtc/common"
)

type RleCompressor struct {
	firstCalled bool
	dataLen     int // every data's len in datas.
	datalenType byte

	datas   [][]byte // all datas.
	lengths []int

	lastData []byte
	length   int

	buf        []byte // bytes will be copy to out.
	rowNumbers uint32

	name string
	perf *common.RtcPerf
}

func NewRleCompressor(dataLen int, name string, perf *common.RtcPerf) *RleCompressor {
	return &RleCompressor{
		datas:       make([][]byte, 0),
		dataLen:     dataLen,
		datalenType: common.DataLenFixed,
		lengths:     make([]int, 0),
		lastData:    nil,
		firstCalled: true,
		length:      0,
		name:        name,
		perf:        perf,
	}
}

func (c *RleCompressor) Compress(src []byte) {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	c.rowNumbers += 1
	if c.firstCalled || reflect.DeepEqual(c.lastData, src) {
		c.length += 1
		c.firstCalled = false
	} else {
		c.datas = append(c.datas, c.lastData)
		c.lengths = append(c.lengths, c.length)
		c.length = 1
	}
	c.lastData = src
	if common.Perf {
		c.perf.AddCpTime(c.name, time.Now().UnixNano()-startTime)
	}
}

func (c *RleCompressor) Finalize(out *[]byte, offset int) int {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	if len(c.datas) != len(c.lengths) {
		panic("rle compressor len(c.datas) != len(c.lengths)")
	}

	if c.lastData != nil {
		c.datas = append(c.datas, c.lastData)
		c.lengths = append(c.lengths, c.length)
	}

	// datas/lengths's length.
	c.buf = binary.LittleEndian.AppendUint32(c.buf, uint32(len(c.datas)))
	for i := 0; i < len(c.datas); i++ {
		c.buf = append(c.buf, c.datas[i]...)
	}
	for i := 0; i < len(c.lengths); i++ {
		c.buf = binary.LittleEndian.AppendUint32(c.buf, uint32(c.lengths[i]))
	}

	// 1. column foramt.
	// tmpOff := offset
	offset = c.finalizeColumnFormat(out, offset, c.datalenType)
	//metaData := offset - tmpOff
	// 2. compressed data.
	copy((*out)[offset:], c.buf)
	offset += len(c.buf)

	// fmt.Println("压缩类型: Rle", "元数据大小：", metaData, "压缩前总长度：", c.rowNumbers*uint32(c.dataLen), "压缩后总长度：", len(c.buf))

	if common.Perf {
		c.perf.AddFTime(c.name, time.Now().UnixNano()-startTime)
	}
	return offset
}

func (r *RleCompressor) finalizeColumnFormat(out *[]byte, offset int, datalenType byte) int {
	// 1. compression type.
	(*out)[offset] = common.ColumnTypeRle
	offset += 1

	// 2. compressed data len.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(r.buf)))
	offset += 4

	// 3. variable or fixed.
	(*out)[offset] = datalenType
	offset += 1
	// 4. row numbers.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], r.rowNumbers)
	offset += 4

	// 5. fixed data len or variable data lens.
	if datalenType == common.DataLenFixed {
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(r.dataLen))
		offset += 4
	} else if datalenType == common.DataLenVariable {
		panic("rle not support variable data len")
	}

	return offset
}

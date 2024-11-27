package lightwightcompression

import (
	"encoding/binary"
	"time"

	"github.com/wqshr12345/golib/compression/rtc/common"
)

type NoCompressor struct {
	datalenType byte // compressor接收到的每个src[]是定长还是变长
	datalen     int  // compressor接收到的每个src[]的固定长度，仅在datalenType为定长时有效
	allLens     []int

	buf        []byte
	rowNumbers uint32

	name string
	perf *common.RtcPerf
}

func NewNoCompressor2(datalenType byte, datalen int, name string, perf *common.RtcPerf) *NoCompressor {
	return &NoCompressor{
		datalenType: datalenType,
		datalen:     datalen,
		buf:         make([]byte, 0),
		rowNumbers:  0,
		name:        name,
		perf:        perf,
	}
}

func NewNoCompressor() *NoCompressor {
	return &NoCompressor{
		buf: make([]byte, 0),
	}
}

func (c *NoCompressor) Compress(src []byte) {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	c.buf = append(c.buf, src...)
	if c.datalenType == common.DataLenVariable {
		c.allLens = append(c.allLens, len(src))
	}
	c.rowNumbers += 1
	if common.Perf {
		c.perf.AddCpTime(c.name, time.Now().UnixNano()-startTime)
	}
}

// TODOIMP(wangqian): 如果引入了column compression，会多至少两次的memcpy...
// 不清楚这个对于整体性能的影响怎么样，可能需要perf看一看...
func (c *NoCompressor) Finalize(out *[]byte, offset int) int {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	// 1. column foramt.
	offset = c.finalizeColumnFormat(out, offset, c.datalenType)
	// 2. compressed data.
	copy((*out)[offset:], c.buf)
	offset += len(c.buf)
	// fmt.Println("压缩类型: No", "压缩前总长度：", c.rowNumbers*uint32(c.datalen), "压缩后总长度：", len(c.buf))

	if common.Perf {
		c.perf.AddFTime(c.name, time.Now().UnixNano()-startTime)
	}
	return offset
}

// TODO(wangqian): 未来可以把这个抽离出来...不要和每一个压缩方法耦合在一起
func (c *NoCompressor) finalizeColumnFormat(out *[]byte, offset int, datalenType byte) int {
	// 1. compression type.
	(*out)[offset] = common.ColumnTypeNone
	offset += 1

	// 2. meta data(标识一些特殊的event，需要做特殊处理) TODO(wangqian)

	// 2. compressed data len.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(c.buf)))
	offset += 4

	// 3. variable or fixed.
	(*out)[offset] = datalenType
	offset += 1
	// 4. row numbers.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], c.rowNumbers)
	offset += 4

	// 5. fixed data len or variable data lens.
	if datalenType == common.DataLenFixed {
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(c.datalen))
		offset += 4
	} else if datalenType == common.DataLenVariable {
		for _, l := range c.allLens {
			binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(l))
			offset += 4
		}
	}

	return offset
}

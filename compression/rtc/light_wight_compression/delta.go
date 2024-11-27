package lightwightcompression

import (
	"encoding/binary"
	"time"

	"github.com/wqshr12345/golib/compression/rtc/common"
)

type DeltaCompressor struct {
	datalenType byte // compressor接收到的每个src[]是定长还是变长
	datalen     int  // compressor接收到的每个src[]的固定长度，仅在datalenType为定长时有效
	buf         []byte
	rowNumbers  uint32

	lastT       uint32
	firstCalled bool

	name string
	perf *common.RtcPerf
}

func NewDeltaCompressor(dataLen int, name string, perf *common.RtcPerf) *DeltaCompressor {
	return &DeltaCompressor{
		datalenType: common.DataLenFixed,
		datalen:     dataLen,
		buf:         make([]byte, 0),
		rowNumbers:  0,
		firstCalled: false,
		name:        name,
		perf:        perf,
	}
}

func (c *DeltaCompressor) Compress(src []byte) {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	c.rowNumbers += 1
	tempT := binary.LittleEndian.Uint32(src)
	if !c.firstCalled {
		c.buf = binary.LittleEndian.AppendUint32(c.buf, tempT)
		c.firstCalled = true
	} else {
		delta := tempT - c.lastT
		c.buf = binary.AppendVarint(c.buf, int64(delta))
	}
	c.lastT = tempT

	if common.Perf {
		c.perf.AddCpTime(c.name, time.Now().UnixNano()-startTime)
	}
}

func (c *DeltaCompressor) Finalize(out *[]byte, offset int) int {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()

	}
	// 1. column foramt.
	//tmpOff := offset
	offset = c.finalizeColumnFormat(out, offset, c.datalenType)
	//metaData := offset - tmpOff
	// 2. compressed data.
	copy((*out)[offset:], c.buf)
	offset += len(c.buf)

	//fmt.Println("压缩类型: Delta", "元数据大小：", metaData, "压缩前总长度：", c.rowNumbers*uint32(c.datalen), "压缩后总长度：", len(c.buf))

	if common.Perf {
		c.perf.AddFTime(c.name, time.Now().UnixNano()-startTime)
	}
	return offset
}

func (r *DeltaCompressor) finalizeColumnFormat(out *[]byte, offset int, datalenType byte) int {
	// 1. compression type.
	(*out)[offset] = common.ColumnTypeDelta
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
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(r.datalen))
		offset += 4
	} else if datalenType == common.DataLenVariable {
		panic("rle not support variable data len")
	}

	return offset
}

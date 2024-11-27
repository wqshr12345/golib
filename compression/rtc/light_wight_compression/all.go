package lightwightcompression

import (
	"encoding/binary"
	"time"
	"unsafe"

	common2 "github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/compression/rtc/common"
)

// 本方法是一个继承所有压缩方法的办法...
// 接收一个vector，其中由一个个pair组成，每个pair都是<percent, compressionType>，用来按比例压缩数据...

type AllCompressor struct {
	// compressor  *zstd.AllCompressor
	cmprPercent []common2.CompressPercent
	offset      int    // 当前buf的偏移
	datalenType byte   // compressor接收到的每个src[]是定长还是变长
	datalen     int    // compressor接收到的每个src[]的固定长度，仅在datalenType为定长时有效
	allLens     []byte // 直接用字节数组存储

	buf        []byte // 用于存储压缩前的数据
	outBuf     []byte // 用于存储压缩后的数据
	rowNumbers uint32

	perf *common.RtcPerf
	name string
}

func (a *AllCompressor) GetBufLen() int {
	return len(a.buf)
}

// func (a *AllCompressor)  {

func NewAllCompressor(cmprPercent []common2.CompressPercent, datalenType byte, datalen int, name string, perf *common.RtcPerf) *AllCompressor {
	return &AllCompressor{
		cmprPercent: cmprPercent,
		offset:      0,
		datalenType: datalenType,
		datalen:     datalen,
		buf:         make([]byte, 0),
		outBuf:      make([]byte, 0),
		rowNumbers:  0,
		name:        name,
		perf:        perf,
	}
}

func (a *AllCompressor) Compress(src []byte) {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	a.buf = append(a.buf, src...)
	if a.datalenType == common.DataLenVariable {
		a.allLens = binary.LittleEndian.AppendUint32(a.allLens, uint32(len(src)))
	}
	a.rowNumbers += 1
	if common.Perf {
		a.perf.AddCpTime(a.name, time.Now().UnixNano()-startTime)
	}
}

// TODOIMP(wangqian): 如果引入了column compression，会多至少两次的memcpy...
// 不清楚这个对于整体性能的影响怎么样，可能需要perf看一看...
func (a *AllCompressor) Finalize(out *[]byte, offset int) int {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	// 1. 首先添加压缩前的数据长度到outBuf中
	originalLen := len(a.buf)
	originalLenBytes := *(*[4]byte)(unsafe.Pointer(&originalLen))
	a.outBuf = append(a.outBuf, originalLenBytes[:]...)
	// 遍历数组内每一个cmpr type，进行压缩
	for i := 0; i < len(a.cmprPercent); i++ {
		// 1. 得到当前压缩类型的一些信息
		cmprType := a.cmprPercent[i].CompressType
		compressor := common.GetCompressorByName(cmprType)
		a.outBuf = append(a.outBuf, cmprType)

		// 2.计算当前应该压缩的长度
		lastOffset := a.offset

		cmprLen := int(float64(len(a.buf)) * a.cmprPercent[i].Percent)
		// TODO 不优雅的实现——当i为最后一个时，默认offset为len(a.buf)
		if i == len(a.cmprPercent)-1 {
			cmprLen = len(a.buf) - a.offset
		}
		intBytes := *(*[4]byte)(unsafe.Pointer(&cmprLen))
		a.offset += cmprLen
		a.outBuf = append(a.outBuf, intBytes[:]...)

		// 3. 添加真正压缩后的数据
		// TODOIMPIMP 这里可以修改Compress内逻辑，把一个dst切片传给它，让它在切片后面append，很重要...
		tempBuf := compressor.Compress(a.buf[lastOffset:a.offset])
		a.outBuf = append(a.outBuf, tempBuf...)

	}
	common.CompressTime += time.Now().UnixNano() - startTime
	// 2. column foramt.
	offset = a.finalizeColumnFormat(out, offset, a.datalenType)
	copy((*out)[offset:], a.buf)
	offset += len(a.buf)
	if common.Perf {
		a.perf.AddFTime(a.name, time.Now().UnixNano()-startTime)
	}
	return offset
}

// TODO(wangqian): 未来可以把这个抽离出来...不要和每一个压缩方法耦合在一起
func (a *AllCompressor) finalizeColumnFormat(out *[]byte, offset int, datalenType byte) int {
	// 1. compression type.
	(*out)[offset] = common.ColumnTypeAll
	offset += 1

	// 2. compressed data len.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(a.buf)))
	offset += 4

	// 3. variable or fixed.
	(*out)[offset] = datalenType
	offset += 1
	// 4. row numbers.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], a.rowNumbers)
	offset += 4

	// 5. fixed data len or variable data lens.
	if datalenType == common.DataLenFixed {
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(a.datalen))
		offset += 4
	} else if datalenType == common.DataLenVariable {
		// TODOIMPIMP 压缩长度默认用Zstd
		compressor := common.ZstdCompressor
		dst := compressor.Compress(a.allLens)
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(dst)))
		offset += 4
		copy((*out)[offset:], dst)
		offset += len(dst)
	}

	return offset
}

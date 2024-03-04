package lightwightcompression

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/wqshr12345/golib/compression/rtc/common"
	"github.com/wqshr12345/golib/compression/zstd"
)

type ZstdCompressor struct {
	compressor  *zstd.ZstdCompressor
	datalenType byte // compressor接收到的每个src[]是定长还是变长
	datalen     int  // compressor接收到的每个src[]的固定长度，仅在datalenType为定长时有效
	buf         []byte
	// allLens     []int
	allLens    []byte // 直接用字节数组存储
	rowNumbers uint32
	// wait       sync.WaitGroup
	nosyncFinished atomic.Bool
	perf           *common.RtcPerf
	name           string
}

func NewZstdCompressor(compressor *zstd.ZstdCompressor, datalenType byte, datalen int, name string, perf *common.RtcPerf) *ZstdCompressor {
	return &ZstdCompressor{
		compressor:  compressor,
		datalenType: datalenType,
		datalen:     datalen,
		buf:         make([]byte, 0), // TODO(wangqian): 预分配buffer大小
		rowNumbers:  0,
		name:        name,
		perf:        perf,
		// wait:        sync.WaitGroup{},
	}
}

func (c *ZstdCompressor) Compress(src []byte) {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	c.buf = append(c.buf, src...)
	if c.datalenType == common.DataLenVariable {
		// c.allLens = append(c.allLens, len(src))
		c.allLens = binary.LittleEndian.AppendUint32(c.allLens, uint32(len(src)))
	}
	c.rowNumbers += 1
	if common.Perf {
		c.perf.AddCpTime(c.name, time.Now().UnixNano()-startTime)
	}
}

func (c *ZstdCompressor) FinalizeNoSync() {
	// defer c.wait.Done()
	// c.wait.Add(1)
	defer common.Wait.Done()
	// common.Wait.Add(1)
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	c.buf = c.compressor.Compress(c.buf)
	// c.nosyncFinished.Store(true)
	if common.Perf {
		c.perf.AddNsfTime(c.name, time.Now().UnixNano()-startTime)
	}
}

func (c *ZstdCompressor) FinalizeSync(out *[]byte, offset int) int {
	// c.wait.Wait()
	// for !c.nosyncFinished.Load() {
	// 	runtime.Gosched()
	// }
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	offset = c.finalizeColumnFormat(out, offset, c.datalenType)
	// metaData := offset - tmpOff
	copy((*out)[offset:], c.buf)
	offset += len(c.buf)
	//fmt.Println("压缩类型: Zstd", "元数据大小: ", metaData, "压缩前总长度：", tmpLen, "压缩后总长度：", len(c.buf), "压缩率：", float64(len(c.buf))/float64(tmpLen))
	if common.Perf {
		c.perf.AddSfTime(c.name, time.Now().UnixNano()-startTime)
	}
	return offset
}

// TODOIMP(wangqian): 如果引入了column compression，会多至少两次的memcpy...
// 不清楚这个对于整体性能的影响怎么样，可能需要perf看一看...
func (c *ZstdCompressor) Finalize(out *[]byte, offset int) int {
	var startTime int64
	// if common.Perf {
	startTime = time.Now().UnixNano()
	// }
	// 1. compressed data.
	// tmpLen := len(c.buf)
	c.buf = c.compressor.Compress(c.buf)
	common.CompressTime += time.Now().UnixNano() - startTime
	// 2. column foramt.
	// tmpOff := offset
	offset = c.finalizeColumnFormat(out, offset, c.datalenType)
	// metaData := offset - tmpOff
	copy((*out)[offset:], c.buf)
	offset += len(c.buf)
	//fmt.Println("压缩类型: Zstd", "元数据大小: ", metaData, "压缩前总长度：", tmpLen, "压缩后总长度：", len(c.buf), "压缩率：", float64(len(c.buf))/float64(tmpLen))
	if common.Perf {
		c.perf.AddFTime(c.name, time.Now().UnixNano()-startTime)
	}
	return offset
}

// TODO(wangqian): 未来可以把这个抽离出来...不要和每一个压缩方法耦合在一起
func (c *ZstdCompressor) finalizeColumnFormat(out *[]byte, offset int, datalenType byte) int {
	// 1. compression type.
	(*out)[offset] = common.ColumnTypeZstd
	offset += 1

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
		compressor := c.compressor
		//fmt.Println("压缩前元信息大小", len(c.allLens))
		dst := compressor.Compress(c.allLens)
		//fmt.Println("压缩后元信息大小: ", len(dst))
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(dst)))
		offset += 4
		copy((*out)[offset:], dst)
		offset += len(dst)
		// copy((*out)[offset:], c.allLens)
		// offset += len(c.allLens)
		// for _, l := range c.allLens {
		// 	// binary.LittleEndian.AppendUint32()
		// 	binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(l))
		// 	offset += 4
		// }
	}

	return offset
}

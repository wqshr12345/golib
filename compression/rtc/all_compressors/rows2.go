package allcompressors

import (
	"encoding/binary"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/wqshr12345/golib/compression/rtc/common"
	lwcompression "github.com/wqshr12345/golib/compression/rtc/light_wight_compression"
)

type SpecCompressor struct {
	columnType byte
	compressor *lwcompression.ZstdCompressor
}

type Row2Compressor struct {
	m              map[uint64][]*SpecCompressor
	wait           sync.WaitGroup
	nosyncFinished atomic.Bool
}

func NewRow2Compressor() *Row2Compressor {
	return &Row2Compressor{
		m: make(map[uint64][]*SpecCompressor),
	}
}

func (r *Row2Compressor) Compress(tableId uint64, columnNums int, includedColumns []byte, columnTypes []byte, src [][]byte) {
	// startTime := time.Now().Unix()
	if _, ok := r.m[tableId]; !ok {
		r.m[tableId] = make([]*SpecCompressor, columnNums)
		for i := 0; i < columnNums; i++ {
			// TODO!(wangqian)(这里暂时全都使用变长...实际上有相当多的数据类型可以做定长)
			name := "table" + strconv.Itoa(int(tableId)) + "column" + strconv.Itoa(i)
			r.m[tableId][i] = &SpecCompressor{
				compressor: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, name, common.RtcPerfs),
				columnType: columnTypes[i],
			}
		}
	}
	for i := 0; i < len(src); i++ {
		// 如果这一列没有被包含，src[i]自然为空，也没有压缩的必要
		if !common.IsBitSet(includedColumns, i) {
			continue
		}
		if columnTypes[i] == common.MYSQL_TYPE_LONGLONG && len(src[i]) == 8 {
			// fmt.Println("table id" + strconv.Itoa(int(tableId)) + "column " + strconv.Itoa(i) + " data is " + strconv.Itoa(int(binary.LittleEndian.Uint64(src[i]))))
		}
		r.m[tableId][i].compressor.Compress(src[i])
	}
	// r.perf.AddCpTime(r.name, time.Now().Unix()-startTime)
}

func (r *Row2Compressor) FinalizeNoSync() {
	// defer r.wait.Done()
	// r.wait.Add(1)
	defer common.Wait.Done()
	// common.Wait.Add(1)
	// startTime := time.Now().Unix()
	for _, compressors := range r.m {
		for _, compressor := range compressors {
			common.Wait.Add(1)
			compressor.compressor.FinalizeNoSync()
		}
	}
	// r.nosyncFinished.Store(true)
	// r.perf.AddNsfTime(r.name, time.Now().Unix()-startTime)
}

func (r *Row2Compressor) FinalizeSync(out *[]byte, offset int) int {
	// r.wait.Wait()
	// for !r.nosyncFinished.Load() {
	// 	runtime.Gosched()
	// }
	// startTime := time.Now().Unix()
	// 1. compression type.
	(*out)[offset] = common.ColumnTypeRows
	offset += 1

	// 2. meta data(标识一些特殊的event，需要做特殊处理) TODO(wangqian)

	// 2. compressed data len.
	// TODO
	// binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(c.buf)))
	offset += 4
	startOffset := offset // 记录起始位置，用于最后计算长度

	// table numbers.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(r.m)))
	offset += 4

	// table info.
	for tableId, compressors := range r.m {
		// table id.
		binary.LittleEndian.PutUint64((*out)[offset:offset+8], tableId)
		//fmt.Println("压缩表id: ", tableId)
		offset += 8
		// table column numbers.
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(compressors)))
		offset += 4
		// column info.
		for _, compressor := range compressors {
			//fmt.Println("压缩列id"+strconv.Itoa(i), "压缩用户列类型: "+common.MysqlTypeToString(compressor.columnType)+"")
			offset = compressor.compressor.FinalizeSync(out, offset)
		}

	}
	binary.LittleEndian.PutUint32((*out)[startOffset-4:startOffset], uint32(offset-startOffset))
	// r.perf.AddSfTime(r.name, time.Now().Unix()-startTime)
	return offset
}

func (r *Row2Compressor) Finalize(out *[]byte, offset int) int {
	// 1. compression type.
	(*out)[offset] = common.ColumnTypeRows
	offset += 1

	// 2. meta data(标识一些特殊的event，需要做特殊处理) TODO(wangqian)

	// 2. compressed data len.
	// TODO
	// binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(c.buf)))
	offset += 4
	startOffset := offset // 记录起始位置，用于最后计算长度

	// table numbers.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(r.m)))
	offset += 4

	// table info.
	for tableId, compressors := range r.m {
		// table id.
		binary.LittleEndian.PutUint64((*out)[offset:offset+8], tableId)
		//fmt.Println("压缩表id: ", tableId)
		offset += 8
		// table column numbers.
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(compressors)))
		offset += 4
		// column info.
		for _, compressor := range compressors {
			//fmt.Println("压缩列id"+strconv.Itoa(i), "压缩用户列类型: "+common.MysqlTypeToString(compressor.columnType)+"")
			offset = compressor.compressor.Finalize(out, offset)
		}

	}
	binary.LittleEndian.PutUint32((*out)[startOffset-4:startOffset], uint32(offset-startOffset))

	return offset
}

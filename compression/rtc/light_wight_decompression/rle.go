package lightwightdecompression

import (
	"encoding/binary"

	"github.com/wqshr12345/golib/compression/rtc/common"
	"github.com/wqshr12345/golib/compression/rtc/event"
)

type RleDecompressor struct {
	// dataLens    []int // 解压后每个数据的大小(var Len)
}

func NewRleDecompressor() *RleDecompressor {
	return &RleDecompressor{}
}

func (r *RleDecompressor) ColumnDecompress(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, eventDataType byte, isEventType bool, dataLenType byte, dataLen int, rowNumbers int, index int) {
	// 1. 从src中解压Dod的数据
	out := r.decompress(src, rowNumbers, dataLen)

	offset := 0
	idx := 0
	for i := 0; i < len(*metas); i++ {
		if eventDataType != event.ALL_TYPES_EVENT && eventDataType != (*metas)[i].EventType {
			continue
		}
		if dataLenType == common.DataLenVariable {
			panic("RleDecompressor ColumnDecompress not support DataLenVariable")
		}
		copy((*dst)[(*metas)[i].Offset:(*metas)[i].Offset+dataLen], out[offset:offset+dataLen])
		if isEventType {
			(*metas)[i].EventType = out[offset]
		}
		if eventDataType == event.WRITE_ROWS_EVENTv0 || eventDataType == event.WRITE_ROWS_EVENTv1 || eventDataType == event.WRITE_ROWS_EVENTv2 {
			// tableid
			if index == 1 {
				for j := 0; j < dataLen; j++ {
					(*metas)[i].TableId += uint64(out[offset+j]) << (8 * j)
				}
			}
			// included columns
			if index == 5 {
				(*metas)[i].IncludedColumns = out[offset : offset+dataLen]
			}
		}
		offset += dataLen
		(*metas)[i].Offset += dataLen
		idx += 1
	}
}

func (r *RleDecompressor) decompress(src []byte, rowNumbers int, dataLen int) []byte {
	uncmprLen := rowNumbers * dataLen

	offset := 0

	dst := make([]byte, 0, uncmprLen)

	// 1. 读取data-length's len
	length := binary.LittleEndian.Uint32(src[offset : offset+4])
	offset += 4
	data := make([][]byte, length)
	lengths := make([]int, length)
	// 2. 读取data
	for i := 0; i < int(length); i++ {
		// 2.1 读取data
		data[i] = src[offset : offset+dataLen]
		offset += dataLen
	}
	// 3. 读取length
	for i := 0; i < int(length); i++ {
		lengths[i] = int(binary.LittleEndian.Uint32(src[offset : offset+4]))
		offset += 4
	}
	for i := 0; i < int(length); i++ {
		for j := 0; j < lengths[i]; j++ {
			dst = append(dst, data[i]...)
		}
	}
	// 3. 读取length
	return dst
}

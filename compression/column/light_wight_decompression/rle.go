package lightwightdecompression

import (
	"github.com/wqshr12345/golib/compression/column/common"
	"github.com/wqshr12345/golib/event"
)

type RleDecompressor struct {
	buf []byte // 存储解压后的数据...
	// dataLens    []int // 解压后每个数据的大小(var Len)
}

func (r *RleDecompressor) ColumnDecompress(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, eventDataType byte, isEventType bool, dataLenType byte, dataLen int, rowNumbers int) {
	// 1. 从src中解压Dod的数据
	out := r.decompress(rowNumbers, dataLen)

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
		offset += dataLen
		(*metas)[i].Offset += dataLen
		idx += 1
	}
}

func (r *RleDecompressor) decompress(rowNumbers int, dataLen int) []byte {
	uncmprLen := rowNumbers * dataLen

	dst := make([]byte, uncmprLen, uncmprLen)

	// 1. 读取data-length's len

	// 2. 读取data

	// 3. 读取length
	return dst
}

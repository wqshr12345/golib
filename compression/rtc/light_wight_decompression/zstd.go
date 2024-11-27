package lightwightdecompression

import (
	"github.com/wqshr12345/golib/compression/rtc/common"
	"github.com/wqshr12345/golib/compression/zstd"
	"github.com/wqshr12345/golib/event"
)

type ZstdDecompressor struct {
	buf []byte // 存储解压后的数据...
}

func NewZstdDecompressor() *ZstdDecompressor {
	return &ZstdDecompressor{
		buf: make([]byte, 0),
	}
}

// TODO (wangqian): 应该把dataLenType、dataLen和dataLens参数放到ColumnDecompress中，而不是放到NewNoDecompressor的参数中
// 为什么要有这个接口？因为行转列压缩的场景，解压不能按照正常逻辑解压，还得把解压后的数据放到每一行的指定位置
// 负责把src中的数据，解压后放到dst中的每一行里面。所以需要额外传入meta信息，用于标识每一行的起始位置。
func (c *ZstdDecompressor) ColumnDecompress(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, eventDataType byte, isEventType bool, dataLenType byte, dataLen int, dataLens []int, index int) {
	out := c.decompress(src)

	offset := 0
	idx := 0
	for i := 0; i < len(*metas); i++ {
		if eventDataType != event.ALL_TYPES_EVENT && eventDataType != (*metas)[i].EventType {
			continue
		}
		if dataLenType == common.DataLenFixed {
		} else if dataLenType == common.DataLenVariable {
			dataLen = dataLens[idx]
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
	// return src
}

// 仅在解压缩Rows这一列的时候才使用这个方法...
func (c *ZstdDecompressor) ColumnDecompressRows(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, eventDataType byte, tableId uint64, isEventType bool, dataLenType byte, dataLen int, dataLens []int, index int) {

	out := c.decompress(src)

	offset := 0
	idx := 0
	for i := 0; i < len(*metas); i++ {
		if eventDataType != event.ALL_TYPES_EVENT && eventDataType != (*metas)[i].EventType {
			continue
		}

		if eventDataType == event.WRITE_ROWS_EVENTv0 || eventDataType == event.WRITE_ROWS_EVENTv1 || eventDataType == event.WRITE_ROWS_EVENTv2 {
			// 1. table不符合，跳过
			if (*metas)[i].TableId != tableId {
				continue
			}
			// 2. table符合，但是included columns不符合，跳过
			if !common.IsBitSet((*metas)[i].IncludedColumns, index) {
				continue
			}
		}

		if dataLenType == common.DataLenFixed {
		} else if dataLenType == common.DataLenVariable {
			dataLen = dataLens[idx]
		}
		copy((*dst)[(*metas)[i].Offset:(*metas)[i].Offset+dataLen], out[offset:offset+dataLen])
		if isEventType {
			(*metas)[i].EventType = out[offset]
		}
		offset += dataLen
		(*metas)[i].Offset += dataLen
		idx += 1
	}
	if offset != len(out) {
		panic("数据没有被完全使用")
	}
	// return src
}

func (c *ZstdDecompressor) ColumnDecompressFirst(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, dataLen int) {

	out := c.decompress(src)

	offset := 0
	for i := 0; i < len(*metas); i++ {
		copy((*dst)[(*metas)[i].Offset:(*metas)[i].Offset+dataLen], out[offset:offset+dataLen])
		var bodyLen uint32
		for j := 0; j < dataLen; j++ {
			bodyLen += uint32(out[offset+j]) << (8 * j)
		}
		offset += dataLen
		(*metas)[i].Offset += dataLen
		if i != len(*metas)-1 {
			(*metas)[i+1].Offset = (*metas)[i].Offset + int(bodyLen) + 1 // 1 is length of sequence number
		}
	}
}

func (c *ZstdDecompressor) decompress(src []byte) []byte {

	dst := zstd.NewDecompressor().Decompress(nil, src)
	return dst
}

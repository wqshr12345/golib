package lightwightdecompression

import (
	"github.com/wqshr12345/golib/compression/column/common"
	"github.com/wqshr12345/golib/event"
)

type NoDecompressor struct {
	buf         []byte // 存储解压后的数据...
	dataLenType byte
	dataLen     int   // 解压后每个数据的大小(fix Len)
	dataLens    []int // 解压后每个数据的大小(var Len)
}

func NewNoDecompressor(dataLenType byte, dataLen int, dataLens []int) *NoDecompressor {
	return &NoDecompressor{
		buf:         make([]byte, 0),
		dataLenType: dataLenType,
		dataLen:     dataLen,
		dataLens:    dataLens,
	}
}

// TODO (wangqian): 应该把dataLenType、dataLen和dataLens参数放到ColumnDecompress中，而不是放到NewNoDecompressor的参数中
// 为什么要有这个接口？因为行转列压缩的场景，解压不能按照正常逻辑解压，还得把解压后的数据放到每一行的指定位置
// 负责把src中的数据，解压后放到dst中的每一行里面。所以需要额外传入meta信息，用于标识每一行的起始位置。
func (c *NoDecompressor) ColumnDecompress(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, eventDataType byte, isEventType bool) {
	// TODO(wangqian): 未来设计别的压缩算法，out应该是src解压后的数据
	out := src
	offset := 0
	dataLen := 0
	idx := 0
	for i := 0; i < len(*metas); i++ {
		if eventDataType != event.ALL_TYPES_EVENT && eventDataType != (*metas)[i].EventType {
			continue
		}
		if c.dataLenType == common.DataLenFixed {
			dataLen = c.dataLen
		} else if c.dataLenType == common.DataLenVariable {
			dataLen = c.dataLens[idx]
		}
		copy((*dst)[(*metas)[i].Offset:(*metas)[i].Offset+dataLen], out[offset:offset+dataLen])
		if isEventType {
			(*metas)[i].EventType = out[offset]
		}
		offset += dataLen
		(*metas)[i].Offset += dataLen
		idx += 1
	}
	// return src
}

// 这个接口用于解析wrapper中的BodyLen字段，然后来确定每一个Wrapper的offset。
func (c *NoDecompressor) ColumnDecompressFirst(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta) {
	// TODO(wangqian): 未来设计别的压缩算法，out应该是src解压后的数据
	out := src
	offset := 0
	dataLen := c.dataLen // 只在用
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

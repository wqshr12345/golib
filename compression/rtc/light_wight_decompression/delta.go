package lightwightdecompression

import (
	"encoding/binary"

	"github.com/wqshr12345/golib/compression/rtc/common"
	"github.com/wqshr12345/golib/compression/rtc/event"
)

type DeltaDecompressor struct {
	buf []byte // 存储解压后的数据
}

func NewDeltaDecompressor() *DeltaDecompressor {
	return &DeltaDecompressor{
		buf: make([]byte, 0),
	}
}

func (c *DeltaDecompressor) ColumnDecompress(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, eventDataType byte, isEventType bool, dataLenType byte, dataLen int, dataLens []int, rowNumbers int) {

	dstLen := dataLen * rowNumbers
	out := c.decompress(src, dstLen)

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
		offset += dataLen
		(*metas)[i].Offset += dataLen
		idx += 1
	}
	// return src
}

func (c *DeltaDecompressor) ColumnDecompressFirst(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, dataLen int) {
	dstLen := dataLen * len(*metas)
	out := c.decompress(src, dstLen)

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

func (c *DeltaDecompressor) decompress(src []byte, dstLen int) []byte {
	dst := make([]byte, 0, dstLen)
	tempT := binary.LittleEndian.Uint32(src[0:4])
	offset := 4
	dst = append(dst, src[0:4]...)
	for offset < len(src) {
		delta, off := binary.Varint(src[offset:])
		tempT += uint32(delta)
		offset += off
		dst = binary.LittleEndian.AppendUint32(dst, tempT)

	}
	return dst
}

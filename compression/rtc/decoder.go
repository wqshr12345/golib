package rtc

import (
	"encoding/binary"

	allcompressors "github.com/wqshr12345/golib/compression/rtc/all_compressors"
	"github.com/wqshr12345/golib/compression/rtc/common"
	lightwightdecompression "github.com/wqshr12345/golib/compression/rtc/light_wight_decompression"
	"github.com/wqshr12345/golib/compression/zstd"
	"github.com/wqshr12345/golib/event"
)

func NewDecompressor() *ColumnDeCompressor {
	return &ColumnDeCompressor{}
}

// TODO(wangqian): 一些设计上的思考，解压的时候其实完全不需要在ColumnDecompressor中维护AllDecompressors，因为解压的时候不需要再上下文中维持状态...
type ColumnDeCompressor struct {
}

// src代表一个或多个完整的压缩后的event。这个语义应该由上层保证。
func (c *ColumnDeCompressor) Decompress(dst, src []byte) []byte { //, testMeta []common.EventWrapperMeta, testCmpr []byte
	total := len(src)
	offset := 0
	// 1. 获得TotalEvents
	totalEvents := binary.LittleEndian.Uint32(src[offset : offset+4])
	offset += 4
	// 1.5 获得OriginalLen
	originalLen := binary.LittleEndian.Uint32(src[offset : offset+4])
	offset += 4
	out := make([]byte, originalLen)

	eventWrapperMetas := make([]common.EventWrapperMeta, totalEvents)

	// 2. 解压元信息——EventWrapper's BodyLen
	offset = c.columnDecompressFirst(src, offset, &out, &eventWrapperMetas)

	// 验证meta
	// for i := 0; i < len(eventWrapperMetas); i++ {
	// 	if eventWrapperMetas[i].Offset != testMeta[i].Offset+3 { // 为什么要加三？因为此时每个body都已经有了3字节的bodylen
	// 		panic("meta error")
	// 	}
	// 	// if eventWrapperMetas[i].EventType != testMeta[i].EventType {
	// 	// 	panic("meta error")
	// 	// }
	// }

	// 3. 解压其它元信息——EventWrapper的其它部分和EventHeader

	// 8代表元数据的列个数——wrapper的Sequence Number、Flag, header的Timestamp、EventType、ServerId、EventLen、NextPos、Flags
	for i := 0; i < 8; i++ {
		isEvnetType := false
		// 好丑陋的写法，先这样吧...
		if i == 3 {
			isEvnetType = true
		}
		offset = c.columnDecompress(src, offset, &out, &eventWrapperMetas, event.ALL_TYPES_EVENT, isEvnetType, i)
	}
	// for i := 0; i < len(eventWrapperMetas); i++ {
	// 	if eventWrapperMetas[i].Offset != testMeta[i].Offset+24 { // 为什么要加24？因为此时每个body都已经有了3字节的bodylen 1字节的seq num 1字节的flag 19字节的event header...
	// 		panic("meta error")
	// 	}
	// 	if eventWrapperMetas[i].EventType != testMeta[i].EventType {
	// 		panic("meta error")
	// 	}
	// 	// 验证event wrapper和event header部分。验证通过...
	// 	for j := 0; j < 24; j++ {
	// 		if out[testMeta[i].Offset+j] != testCmpr[testMeta[i].Offset+j] {
	// 			panic("meta error")
	// 		}
	// 	}
	// }
	// 2. 解压数据部分——EventData
	for offset < total {
		offset = c.columnDecompressEventData(src, offset, &out, &eventWrapperMetas)
	}
	// 验证out和testCmpr
	// for i := 0; i < len(out); i++ {
	// 	if out[i] != testCmpr[i] {
	// 		panic("data error")
	// 	}
	// }

	return out
}

// 解压wrapper中的BodyLen。以此确定每个EventWrapper的offset
func (c *ColumnDeCompressor) columnDecompressFirst(src []byte, offset int, dst *[]byte, metas *[]common.EventWrapperMeta) int {
	// 1. 获得元数据
	columnType := src[offset]
	offset += 1

	columnLen := binary.LittleEndian.Uint32(src[offset : offset+4])
	offset += 4

	datalenType := src[offset]
	offset += 1

	rowNumbers := binary.LittleEndian.Uint32(src[offset : offset+4])
	offset += 4

	var dataLen int
	var dataLens []int

	if datalenType == common.DataLenFixed {
		dataLen = int(binary.LittleEndian.Uint32(src[offset : offset+4]))
		offset += 4
	} else if datalenType == common.DataLenVariable {
		dataLens = make([]int, 0, rowNumbers)
		len := binary.LittleEndian.Uint32(src[offset : offset+4])
		offset += 4
		decompressor := zstd.NewDecompressor()
		dst := decompressor.Decompress(nil, src[offset:offset+int(len)])
		for i := 0; i < int(rowNumbers); i++ {
			dataLens = append(dataLens, int(binary.LittleEndian.Uint32(dst[i*4:i*4+4])))
		}
		offset += int(len)
		// for i := 0; i < int(rowNumbers); i++ {
		// 	dataLens = append(dataLens, int(binary.LittleEndian.Uint32(src[offset:offset+4])))
		// 	offset += 4
		// }
	}

	// 2. 解压BodyLen数据
	if columnType == common.ColumnTypeNone {
		decompressor := lightwightdecompression.NewNoDecompressor()
		decompressor.ColumnDecompressFirst(src[offset:offset+int(columnLen)], dst, metas, datalenType, dataLen, dataLens)
	} else if columnType == common.ColumnTypeZstd {
		decompressor := lightwightdecompression.NewZstdDecompressor()
		decompressor.ColumnDecompressFirst(src[offset:offset+int(columnLen)], dst, metas, dataLen)
	} else {
		panic("not support yet")
	}
	offset += int(columnLen)

	return offset
}

// 解压wrapper和header中的其它部分
func (c *ColumnDeCompressor) columnDecompress(src []byte, offset int, dst *[]byte, metas *[]common.EventWrapperMeta, eventDataType byte, isEventType bool, i int) int {

	// 1. 获得一些元数据
	columnType := src[offset]
	offset += 1

	columnLen := binary.LittleEndian.Uint32(src[offset : offset+4])
	offset += 4

	if columnType == common.ColumnTypeRows {
		decompressor := allcompressors.NewRowsDecompressor()
		decompressor.ColumnDecompress(src[offset:offset+int(columnLen)], dst, metas, eventDataType, isEventType)
		offset += int(columnLen)
		return offset
	}
	datalenType := src[offset]
	offset += 1

	rowNumbers := binary.LittleEndian.Uint32(src[offset : offset+4])
	offset += 4

	var dataLen int
	var dataLens []int
	if datalenType == common.DataLenFixed {
		dataLen = int(binary.LittleEndian.Uint32(src[offset : offset+4]))
		offset += 4
	} else if datalenType == common.DataLenVariable {
		len := binary.LittleEndian.Uint32(src[offset : offset+4])
		offset += 4
		decompressor := zstd.NewDecompressor()
		dst := decompressor.Decompress(nil, src[offset:offset+int(len)])
		for i := 0; i < int(rowNumbers); i++ {
			dataLens = append(dataLens, int(binary.LittleEndian.Uint32(dst[i*4:i*4+4])))
		}
		offset += int(len)
		// for i := 0; i < int(rowNumbers); i++ {
		// 	dataLens = append(dataLens, int(binary.LittleEndian.Uint32(src[offset:offset+4])))
		// 	offset += 4
		// }
	}

	// 2. 解压数据
	if columnType == common.ColumnTypeNone {
		decompressor := lightwightdecompression.NewNoDecompressor()
		decompressor.ColumnDecompress(src[offset:offset+int(columnLen)], dst, metas, eventDataType, isEventType, datalenType, dataLen, dataLens)
	} else if columnType == common.ColumnDod {
		decompressor := lightwightdecompression.NewDodDecompressor()
		decompressor.ColumnDecompress(src[offset:offset+int(columnLen)], dst, metas, eventDataType, isEventType, datalenType, dataLen, int(rowNumbers))
	} else if columnType == common.ColumnTypeRle {
		decompressor := lightwightdecompression.NewRleDecompressor()
		decompressor.ColumnDecompress(src[offset:offset+int(columnLen)], dst, metas, eventDataType, isEventType, datalenType, dataLen, int(rowNumbers), i)
	} else if columnType == common.ColumnTypeZstd {
		decompressor := lightwightdecompression.NewZstdDecompressor()
		decompressor.ColumnDecompress(src[offset:offset+int(columnLen)], dst, metas, eventDataType, isEventType, datalenType, dataLen, dataLens, i)
	} else if columnType == common.ColumnTypeDelta {
		decompressor := lightwightdecompression.NewDeltaDecompressor()
		decompressor.ColumnDecompress(src[offset:offset+int(columnLen)], dst, metas, eventDataType, isEventType, datalenType, dataLen, dataLens, int(rowNumbers))
	} else {
		panic("ColumnType not support yet")
	}
	offset += int(columnLen)
	return offset
}

func (c *ColumnDeCompressor) columnDecompressEventData(src []byte, offset int, dst *[]byte, metas *[]common.EventWrapperMeta) int {
	// 1. 获得event type元数据
	eventType := src[offset]
	offset += 1

	eventLen := binary.LittleEndian.Uint32(src[offset : offset+4])
	offset += 4

	endOff := offset + int(eventLen)
	// 2. 解压不同列的数据

	// TODO(wangqian) 后续的解压event data其实可以并行化... 用很多个go routine来解压
	i := 0
	for offset < endOff {
		i += 1
		offset = c.columnDecompress(src, offset, dst, metas, eventType, false, i)
	}

	return offset
}

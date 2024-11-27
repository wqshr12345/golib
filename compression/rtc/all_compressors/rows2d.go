package allcompressors

import (
	"encoding/binary"

	"github.com/wqshr12345/golib/compression/rtc/common"
	lightwightdecompression "github.com/wqshr12345/golib/compression/rtc/light_wight_decompression"
	"github.com/wqshr12345/golib/compression/zstd"
)

type RowsDecompressor struct {
	// m map[uint64][]*lwdecompression.ZstdDecompressor
}

func NewRowsDecompressor() *RowsDecompressor {
	return &RowsDecompressor{
		// m: make(map[uint64][]*lwdecompression.ZstdDecompressor),
	}
}

func (r *RowsDecompressor) ColumnDecompress(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, eventDataType byte, isEventType bool) {
	offset := 0
	tableNums := binary.LittleEndian.Uint32(src[offset : offset+4])
	offset += 4
	for ti := 0; ti < int(tableNums); ti++ {
		tableId := binary.LittleEndian.Uint64(src[offset : offset+8])
		offset += 8
		columnNums := binary.LittleEndian.Uint32(src[offset : offset+4])
		offset += 4
		for ci := 0; ci < int(columnNums); ci++ {
			// 下面的代码照抄 columnDecompress()部分
			// 1. 获得一些元数据
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
			// Note(wangqian): 如果rownumbers为0，那么就不需要后续流程判断
			// if rowNumbers == 0 {
			// 	continue
			// }
			if columnType == common.ColumnTypeZstd {
				decompressor := lightwightdecompression.NewZstdDecompressor()
				decompressor.ColumnDecompressRows(src[offset:offset+int(columnLen)], dst, metas, eventDataType, tableId, isEventType, datalenType, dataLen, dataLens, ci)
			} else {
				panic("not support yet")
			}
			offset += int(columnLen)
		}
	}
}

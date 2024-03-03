package lightwightcompression

import "github.com/wqshr12345/golib/compression/zstd"

type ZstdCompressor struct {
	compressor  *zstd.ZstdCompressor
	datalenType byte // compressor接收到的每个src[]是定长还是变长
	datalen     int  // compressor接收到的每个src[]的固定长度，仅在datalenType为定长时有效
	buf         []byte
	allLens     []byte // 直接用字节数组存储
	rowNumbers  uint32
}

func NewZstdCompressor(compressor *zstd.ZstdCompressor, datalenType byte, datalen int) *ZstdCompressor {
	return &ZstdCompressor{
		compressor:  compressor,
		datalenType: datalenType,
		datalen:     datalen,
		buf:         make([]byte, 0), // TODO wq 预分配内存大小
		allLens:     make([]byte, 0),
		rowNumbers:  0,
	}
}


func （）
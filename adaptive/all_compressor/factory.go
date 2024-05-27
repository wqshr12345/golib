package allcompressor

import "github.com/wqshr12345/golib/common"

type AllCompressor struct {
	zstdCompressor   *ZstdCompressor
	lz4Compressor    *Lz4Compressor
	snappyCompressor *SnappyCompressor
	noCompressor     *NoCompressor
	LzoCompressor    *LzoCompressor
	GzipCompressor   *GzipCompressor
	FlateCompressor  *FlateCompressor
	XzCompressor     *XzCompressor
}

func NewAllCompressor() *AllCompressor {
	return &AllCompressor{
		zstdCompressor:   NewZstdCompressor(),
		lz4Compressor:    NewLz4Compressor(),
		snappyCompressor: NewSnappyCompressor(),
		noCompressor:     NewNoCompressor(),
		LzoCompressor:    NewLzoCompressor(),
		GzipCompressor:   NewGzipCompressor(),
		FlateCompressor:  NewFlateCompressor(),
		XzCompressor:     NewXzCompressor(),
	}

}

func (a *AllCompressor) GetCompressorByType(cmprType byte) common.Compressor {
	switch cmprType {
	case common.ZSTD:
		return a.zstdCompressor
	case common.LZ4:
		return a.lz4Compressor
	case common.SNAPPY:
		return a.snappyCompressor
	case common.NOCOMPRESSION:
		return a.noCompressor
	case common.GZIP:
		return a.GzipCompressor
	case common.LZO:
		return a.LzoCompressor
	case common.FLATE:
		return a.FlateCompressor
	// case common.XZ:
	// return a.XzCompressor
	// case common.RLE:
	// 	// TODO 选定数据长度
	// 	return NewRleCompressor(4)
	// case common.DELTA:
	// 	// TODO 选择数据长度
	// 	return NewDeltaCompressor(4)
	default:
		panic("invalid cmprType")
	}
}

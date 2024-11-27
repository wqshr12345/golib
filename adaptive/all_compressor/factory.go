package allcompressor

import "github.com/wqshr12345/golib/common"

type AllCompressor struct {
	zstdCompressor1  *ZstdCompressor
	zstdCompressor3  *ZstdCompressor
	zstdCompressor8  *ZstdCompressor
	zstdCompressor22 *ZstdCompressor
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
		zstdCompressor1:  NewZstdCompressor(1),
		zstdCompressor3:  NewZstdCompressor(3),
		zstdCompressor8:  NewZstdCompressor(8),
		zstdCompressor22: NewZstdCompressor(22),
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
	case common.ZSTD1:
		return a.zstdCompressor1
	case common.ZSTD:
		return a.zstdCompressor3
	case common.ZSTD8:
		return a.zstdCompressor8
	case common.ZSTD22:
		return a.zstdCompressor22
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

type AllDecompressor struct {
	zstdDecompressor   *ZstdDecompressor
	lz4Decompressor    *Lz4Decompressor
	snappyDecompressor *SnappyDecompressor
	noDecompressor     *NoDecompressor
	LzoDecompressor    *LzoDecompressor
	GzipDecompressor   *GzipDecompressor
	FlateDecompressor  *FlateDecompressor
	XzDecompressor     *XzDecompressor
}

func NewAllDecompressor() *AllDecompressor {
	return &AllDecompressor{
		zstdDecompressor:   NewZstdDecompressor(),
		lz4Decompressor:    NewLz4Decompressor(),
		snappyDecompressor: NewSnappyDecompressor(),
		noDecompressor:     NewNoDecompressor(),
		LzoDecompressor:    NewLzoDecompressor(),
		GzipDecompressor:   NewGzipDecompressor(),
		FlateDecompressor:  NewFlateDecompressor(),
		XzDecompressor:     NewXzDecompressor(),
	}

}

func (a *AllDecompressor) GetDecompressorByType(cmprType byte) common.Decompressor {
	switch cmprType {
	case common.ZSTD1:
		return a.zstdDecompressor
	case common.ZSTD:
		return a.zstdDecompressor
	case common.ZSTD8:
		return a.zstdDecompressor
	case common.ZSTD22:
		return a.zstdDecompressor
	case common.LZ4:
		return a.lz4Decompressor
	case common.SNAPPY:
		return a.snappyDecompressor
	case common.NOCOMPRESSION:
		return a.noDecompressor
	case common.GZIP:
		return a.GzipDecompressor
	case common.LZO:
		return a.LzoDecompressor
	case common.FLATE:
		return a.FlateDecompressor
	// case common.XZ:
	// return a.XzDecompressor
	// case common.RLE:
	// 	// TODO 选定数据长度
	// 	return NewRleDecompressor(4)
	// case common.DELTA:
	// 	// TODO 选择数据长度
	// 	return NewDeltaDecompressor(4)
	default:
		panic("invalid cmprType")
	}
}

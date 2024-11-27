package adaptive

import (
	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/compression/brotil"
	"github.com/wqshr12345/golib/compression/flate"
	"github.com/wqshr12345/golib/compression/lz4"
	"github.com/wqshr12345/golib/compression/no"
	"github.com/wqshr12345/golib/compression/rtc"
	"github.com/wqshr12345/golib/compression/snappy"
	"github.com/wqshr12345/golib/compression/zstd"
)

// func NewCompressor(commonType uint8) common.Compressor {
// 	switch commonType {
// 	case common.CompressTypeZstd:
// 		return zstd.NewCompressor()
// 	case common.CompressTypeSnappy:
// 		return snappy.NewCompressor()
// 	case common.CompressTypeNone:
// 		return no.NewCompressor()
// 	case common.CompressTypeLz4:
// 		return lz4.NewCompressor()

// 	case common.CompressTypeFlate:
// 		return flate.NewCompressor()
// 	case common.CompressTypeBrotli:
// 		return brotil.NewCompressor()
// 	case common.CompressTypeRtc:
// 		return rtc.NewRtcCompressor()
// 	default:
// 		panic("invalid compress type")

// 	}
// }

func NewDecompressor(commonType uint8) common.Decompressor {
	switch commonType {
	case common.CompressTypeZstd:
		return zstd.NewDecompressor()
	case common.CompressTypeSnappy:
		return snappy.NewDecompressor()
	case common.CompressTypeNone:
		return no.NewDecompressor()
	case common.CompressTypeLz4:
		return lz4.NewDecompressor()

	case common.CompressTypeFlate:
		return flate.NewDecompressor()
	case common.CompressTypeBrotli:
		return brotil.NewDecompressor()
	case common.CompressTypeRtc:
		return rtc.NewDecompressor()
	default:
		panic("invalid compress type")

	}
}

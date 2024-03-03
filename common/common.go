package common

import "fmt"

type CompressInfo struct {
	PkgId          int
	CompressType   int
	DataLen        int //original data
	CompressTime   int64
	TranportTime   int64
	DecompressTime int64
	CompressRatio  float64
}

type ReportFunction func(CompressInfo)

const (
	CompressTypeNovalid = iota
	CompressTypeNone
	CompressTypeLz4
	CompressTypeSnappy
	CompressTypeZstd
	CompressTypeGzip
	CompressTypeBzip2
	CompressTypeFlate
	CompressTypeZlib
	CompressTypeLzw
	CompressTypeBrotli
	CompressTypeRtc
)

func GetCompressionType(compressTypeStr string) (uint8, error) {
	switch compressTypeStr {
	case "None":
		return CompressTypeNone, nil
	case "Lz4":
		return CompressTypeLz4, nil
	case "Snappy":
		return CompressTypeSnappy, nil
	case "Zstd":
		return CompressTypeZstd, nil
	case "Gzip":
		return CompressTypeGzip, nil
	case "Bzip2":
		return CompressTypeBzip2, nil
	case "Flate":
		return CompressTypeFlate, nil
	case "Zlib":
		return CompressTypeZlib, nil
	case "Lzw":
		return CompressTypeLzw, nil
	case "Brotli":
		return CompressTypeBrotli, nil
	case "Rtc":
		return CompressTypeRtc, nil
	default:
		return CompressTypeNovalid, fmt.Errorf("invalid compress type: %s", compressTypeStr)
	}
}

type WriteFlusher interface {
	Write(b []byte) (n int, err error)
	Flush() error
}

type Compressor interface {
	Compress([]byte) []byte
}
type Decompressor interface {
	Decompress([]byte, []byte) []byte
}

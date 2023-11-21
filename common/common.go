package common

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
	CompressTypeNone   = uint8(0)
	CompressTypeSnappy = uint8(1)
	CompressTypeZstd   = uint8(2)
	CompressTypeGzip   = uint8(3)
	CompressTypeBzip2  = uint8(4)
	CompressTypeFlate  = uint8(5)
	CompressTypeZlib   = uint8(6)
	CompressTypeLzw    = uint8(7)
)

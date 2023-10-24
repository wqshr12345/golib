package adaptive

type CompressInfo struct {
	pkgId          int
	compressType   int
	compressTime   int64
	tranportTime   int64
	decompressTime int64
	compressRatio  float64
}

type ReportFunction func(CompressInfo)

type Reporter interface {
	Report(CompressInfo)
}

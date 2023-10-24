package adaptive

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

type Reporter interface {
	Report(CompressInfo)
}

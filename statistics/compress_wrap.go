package statistics

import (
	"github.com/wqshr12345/golib/compression"
	"os"
	"time"
)

type CompressWrap struct {
	compressor compression.Compressor
	stat       bandwidth
}

func NewCompressWrap(c compression.Compressor, output *os.File) *CompressWrap {
	return &CompressWrap{
		compressor: c,
		stat:       *NewBandwidthStatistics(output),
	}
}

func (cw *CompressWrap) Compress(s []byte) (d []byte) {
	st := time.Now()
	d = cw.compressor.Compress(s)
	cw.stat.AddCompressBandwidth(len(s), st)
	return
}

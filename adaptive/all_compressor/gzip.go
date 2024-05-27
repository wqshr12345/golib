package allcompressor

import (
	"bytes"
	"compress/gzip"
)

func NewGzipCompressor() *GzipCompressor {
	return &GzipCompressor{}
}

type GzipCompressor struct {
}

func (g *GzipCompressor) Compress(src []byte) []byte {
	var buf bytes.Buffer
	encoder := gzip.NewWriter(&buf)
	encoder.Write(src)
	encoder.Close()
	return buf.Bytes()
}

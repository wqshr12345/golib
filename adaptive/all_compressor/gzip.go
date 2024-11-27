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

func NewGzipDecompressor() *GzipDecompressor {
	return &GzipDecompressor{}
}

type GzipDecompressor struct {
}

func (g *GzipDecompressor) Decompress(dst, src []byte) []byte {
	r, _ := gzip.NewReader(bytes.NewReader(src))
	n, err := r.Read(dst)
	if err != nil {
		panic(err)
	}
	totalLen := n
	for n != 0 {
		n, _ = r.Read(dst[totalLen:])
		// if err != nil && err != {
		// panic(err)
		// }
		totalLen += n
	}
	// r.Close()
	return dst[:totalLen]
}

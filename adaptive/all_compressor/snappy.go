package allcompressor

import (
	"github.com/golang/snappy"
)

func NewSnappyCompressor() *SnappyCompressor {
	return &SnappyCompressor{}
}

type SnappyCompressor struct {
}

// this will return a snappy format data.
func (c *SnappyCompressor) Compress(src []byte) []byte {
	return snappy.Encode(nil, src)
}

func NewSnappyDecompressor() *SnappyDecompressor {
	return &SnappyDecompressor{}
}

type SnappyDecompressor struct {
}

func (c *SnappyDecompressor) Decompress(dst, src []byte) []byte {
	ans, _ := snappy.Decode(dst, src)
	return ans
}

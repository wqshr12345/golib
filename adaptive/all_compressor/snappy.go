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

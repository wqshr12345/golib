package snappy

import (
	"github.com/golang/snappy"
)

func NewDecompressor() *SnappyDecompressor {
	return &SnappyDecompressor{}
}

type SnappyDecompressor struct {
}

// this will return a original data.
func (c *SnappyDecompressor) Decompress(dst, src []byte) []byte {
	ans, err := snappy.Decode(dst, src)
	if err != nil {
		panic(err)
	}
	return ans
}

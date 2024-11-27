package lz4

import (
	"github.com/pierrec/lz4/v4"
)

func NewCompressor() *Lz4Compressor {
	return &Lz4Compressor{}
}

type Lz4Compressor struct {
}

func (c *Lz4Compressor) Compress(src []byte) []byte {
	dst := make([]byte, lz4.CompressBlockBound(len(src)))

	dstSize, err := lz4.CompressBlock(src, dst, nil)
	if err != nil {
		panic(err)
	}
	return dst[:dstSize]
}

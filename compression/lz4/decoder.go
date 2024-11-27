package lz4

import (
	"github.com/pierrec/lz4/v4"
)

func NewDecompressor() *Lz4Decompressor {
	return &Lz4Decompressor{}
}

type Lz4Decompressor struct {
}

func (c *Lz4Decompressor) Decompress(dst, src []byte) []byte {
	usz, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		panic(err)
	}
	return dst[:usz]
}

package allcompressor

import "github.com/pierrec/lz4"

func NewLz4Compressor() *Lz4Compressor {
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

func NewLz4Decompressor() *Lz4Decompressor {
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

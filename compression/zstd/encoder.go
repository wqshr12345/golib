package zstd

import (
	"github.com/klauspost/compress/zstd"
)

func NewCompressor() *ZstdCompressor {

	encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	return &ZstdCompressor{
		encoder: encoder,
	}
}

type ZstdCompressor struct {
	encoder *zstd.Encoder
}

// this will return a zstd format data.
func (c *ZstdCompressor) Compress(src []byte) []byte {
	return c.encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

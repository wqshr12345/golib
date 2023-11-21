package zstd

import (
	"github.com/wqshr12345/compress/zstd"
)

func NewCompressor() *ZstdCompressor {
	encoder, _ := zstd.NewWriter(nil)
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

package all

import (
	"github.com/klauspost/compress/zstd"
)

func NewCompressor() *AllCompressor {
	encoder, _ := zstd.NewWriter(nil)
	return &AllCompressor{
		zstd_encoder: encoder,
	}
}

type AllCompressor struct {
	zstd_encoder *zstd.Encoder
}

// this will return a zstd format data.
func (c *AllCompressor) Compress(src []byte) []byte {
	return c.zstd_encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

package allcompressor

import "github.com/klauspost/compress/zstd"

func NewZstdCompressor(level int) *ZstdCompressor {
	//  zstd.NewWriter(nil)
	var zstdLevel zstd.EncoderLevel
	if level == 1 {
		zstdLevel = zstd.SpeedFastest
	} else if level == 3 {
		zstdLevel = zstd.SpeedDefault
	} else if level == 8 {
		zstdLevel = zstd.SpeedBetterCompression
	} else if level == 22 {
		zstdLevel = zstd.SpeedBestCompression
	}
	encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstdLevel))
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

func NewZstdDecompressor() *ZstdDecompressor {
	decoder, _ := zstd.NewReader(nil)
	return &ZstdDecompressor{
		decoder: decoder,
	}
}

type ZstdDecompressor struct {
	decoder *zstd.Decoder
}

// this will return a original data.
func (c *ZstdDecompressor) Decompress(dst, src []byte) []byte {
	ans, err := c.decoder.DecodeAll(src, dst)
	if err != nil {
		panic(err)
	}
	return ans
}

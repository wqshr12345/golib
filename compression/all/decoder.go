package all
import (
	"github.com/klauspost/compress/zstd"
)

func NewDecompressor() *ZstdDecompressor {
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
package no

type NoDecompressor struct {
}

func (c *NoDecompressor) Decompress(dst, src []byte) []byte {
	return src
}

func NewDecompressor() *NoDecompressor {
	return &NoDecompressor{}
}

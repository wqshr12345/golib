package no

type NoCompressor struct {
}

func (c *NoCompressor) Compress(src []byte) []byte {
	return src
}

func NewCompressor() *NoCompressor {
	return &NoCompressor{}
}

package allcompressor

type NoCompressor struct {
}

func NewNoCompressor() *NoCompressor {
	return &NoCompressor{}
}

func (c *NoCompressor) Compress(src []byte) []byte {
	return src
}

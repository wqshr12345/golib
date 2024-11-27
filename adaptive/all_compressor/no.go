package allcompressor

type NoCompressor struct {
}

func NewNoCompressor() *NoCompressor {
	return &NoCompressor{}
}

func (c *NoCompressor) Compress(src []byte) []byte {
	return src
}

type NoDecompressor struct {
}

func NewNoDecompressor() *NoDecompressor {
	return &NoDecompressor{}
}

func (c *NoDecompressor) Decompress(dst, src []byte) []byte {
	return src
}

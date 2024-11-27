package compression

type Compressor interface {
	Compress([]byte) []byte
}

type Decompressor interface {
	Decompress([]byte, []byte) []byte
}

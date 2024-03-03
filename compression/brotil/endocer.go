package brotil

import (
	"bytes"

	brotil "github.com/andybalholm/brotli"
)

func NewCompressor() *BrotliCompressor {
	return &BrotliCompressor{}
}

type BrotliCompressor struct {
}

func (c *BrotliCompressor) Compress(src []byte) []byte {
	var buf bytes.Buffer
	w := brotil.NewWriterLevel(&buf, brotil.DefaultCompression)
	w.Write(src)
	w.Close()
	return buf.Bytes()
}

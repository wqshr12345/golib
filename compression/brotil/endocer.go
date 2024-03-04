package brotil

import (
	"bytes"

	"github.com/andybalholm/brotli"
)

func NewCompressor() *BrotliCompressor {
	return &BrotliCompressor{}
}

type BrotliCompressor struct {
}

func (c *BrotliCompressor) Compress(src []byte) []byte {
	var buf bytes.Buffer
	w := brotli.NewWriterLevel(&buf, brotli.DefaultCompression)
	w.Write(src)
	w.Close()
	return buf.Bytes()
}
